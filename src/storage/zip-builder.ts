// zip-builder.ts
import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  HeadObjectCommand,
} from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import * as archiver from "archiver";
import { PassThrough } from "stream";
import { pipeline } from "stream/promises";
import { posix as pathPosix } from "node:path";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import { Agent as HttpsAgent } from "https";
import { envInt } from "../env";

const CONNECT_TIMEOUT_MS = envInt("S3_CONNECT_TIMEOUT_MS", 5_000);
const SOCKET_TIMEOUT_MS = envInt("S3_SOCKET_TIMEOUT_MS", 60_000);
const PREFETCH_CONCURRENCY = envInt("S3_PREFETCH_CONCURRENCY", 6);
// Hard upper bound on the entire zip build operation. Without this, a slow
// or unresponsive Wasabi GetObject can hang the worker indefinitely — Bull's
// stall detector eventually kills it, but heartbeats stay alive in the
// meantime so we don't notice. Default 30 minutes; tune via env.
const MAX_ZIP_DURATION_MS = envInt("ZIP_MAX_DURATION_MS", 30 * 60 * 1000);
// Caps on fetching an extra entry (the pack licence). Deliberately tight:
// it is a document, not media, and it must never be the reason a zip stalls.
const EXTRA_ENTRY_TIMEOUT_MS = envInt("ZIP_EXTRA_ENTRY_TIMEOUT_MS", 15_000);
const EXTRA_ENTRY_MAX_BYTES = envInt(
  "ZIP_EXTRA_ENTRY_MAX_BYTES",
  10 * 1024 * 1024
);

const httpHandler = new NodeHttpHandler({
  httpsAgent: new HttpsAgent({
    keepAlive: true,
    keepAliveMsecs: 10_000,
    maxSockets: envInt("S3_MAX_SOCKETS", 512),
    maxFreeSockets: envInt("S3_MAX_FREE_SOCKETS", 128),
  }),
  connectionTimeout: CONNECT_TIMEOUT_MS,
  socketTimeout: SOCKET_TIMEOUT_MS,
});

export interface ZipFileDescriptor {
  filePath: string;
  fileName?: string;
  relPath?: string;
}

/**
 * An entry that isn't a member file — today, the pack licence. Either literal
 * text the caller generated, or a URL to fetch.
 *
 * A URL rather than an S3 key on purpose: this worker only holds credentials
 * for the zip's own bucket, and the artist's licence PDF lives in the public
 * one. Fetching over HTTPS keeps that boundary intact.
 */
export interface ZipExtraEntry {
  fileName: string;
  text?: string;
  url?: string;
}

export interface ZipBuildResult {
  finalKey: string;
  totalFiles: number;
  addedFiles: number;
  skippedFiles: Array<{ filePath: string; reason: string }>;
  /**
   * How many extra entries (today: the pack licence) actually made it in.
   * Reported separately from the file counts because the caller compares
   * those against the list it sent — but it still needs to know whether the
   * licence shipped, and an absent field is how it tells a worker that
   * predates extras from one that dropped them.
   */
  addedExtraEntries: number;
}

export interface ZipBuilderConfig {
  endpoint: string;
  region: string;
  bucket: string;
  accessKeyId: string;
  secretAccessKey: string;
}

interface PrefetchedFile {
  kind: "ok";
  descriptor: ZipFileDescriptor;
  s3Key: string;
  data: Buffer;
  lastModified: Date;
}

interface SkippedFile {
  kind: "skipped";
  filePath: string;
  reason: string;
}

function isNoSuchKey(err: any) {
  return (
    err?.name === "NoSuchKey" ||
    err?.Code === "NoSuchKey" ||
    err?.code === "NoSuchKey" ||
    err?.$metadata?.httpStatusCode === 404
  );
}

export type StorageProbeResult = {
  ok: boolean;
  at: string;
  roundTripMs: number | null;
  /**
   * Result of HEAD-ing the caller-provided canary key. The write/read probe
   * proves the credentials work against SOME bucket; only the canary — a key
   * the backend recently wrote — proves it is the SAME bucket the backend
   * writes to. `null` when no canary key is configured.
   */
  canary: { key: string; found: boolean } | null;
  error: string | null;
};

export class ZipBuilder {
  private s3: S3Client;
  private bucket: string;
  private endpoint: string;

  constructor(cfg: ZipBuilderConfig) {
    if (!cfg.accessKeyId || !cfg.secretAccessKey) {
      throw new Error(
        "ZipBuilder: missing accessKeyId/secretAccessKey – check env vars"
      );
    }

    const MAX_ATTEMPTS = envInt("S3_MAX_ATTEMPTS", 4);

    this.bucket = cfg.bucket;
    this.endpoint = cfg.endpoint;
    this.s3 = new S3Client({
      region: cfg.region,
      endpoint: cfg.endpoint,
      forcePathStyle: true,
      requestHandler: httpHandler,
      maxAttempts: MAX_ATTEMPTS,
      retryMode: "standard",

      credentials: {
        accessKeyId: cfg.accessKeyId,
        secretAccessKey: cfg.secretAccessKey,
      },
    });
  }

  get bucketName() {
    return this.bucket;
  }

  get endpointHost() {
    try {
      return new URL(this.endpoint).host;
    } catch {
      return this.endpoint;
    }
  }

  /**
   * Prove the configured storage actually works: write a tiny probe object,
   * read it back, and (when given one) HEAD a canary key the backend wrote.
   * A worker that passes write/read but fails the canary is talking to the
   * WRONG bucket — exactly the misconfiguration that broke pack downloads.
   */
  async probeStorage(canaryKey?: string): Promise<StorageProbeResult> {
    const at = new Date().toISOString();
    const started = Date.now();
    const probeKey = "_health/zip-worker-probe.txt";

    try {
      await this.s3.send(
        new PutObjectCommand({
          Bucket: this.bucket,
          Key: probeKey,
          Body: `probe ${at}`,
          ContentType: "text/plain",
        })
      );
      const obj = await this.s3.send(
        new GetObjectCommand({ Bucket: this.bucket, Key: probeKey })
      );
      await obj.Body!.transformToByteArray();

      let canary: StorageProbeResult["canary"] = null;
      if (canaryKey) {
        try {
          await this.s3.send(
            new HeadObjectCommand({
              Bucket: this.bucket,
              Key: this.normalizeKey(canaryKey),
            })
          );
          canary = { key: canaryKey, found: true };
        } catch (err: any) {
          if (isNoSuchKey(err)) {
            canary = { key: canaryKey, found: false };
          } else {
            throw err;
          }
        }
      }

      return {
        ok: canary ? canary.found : true,
        at,
        roundTripMs: Date.now() - started,
        canary,
        error: canary && !canary.found ? "canary_key_not_found" : null,
      };
    } catch (err: any) {
      return {
        ok: false,
        at,
        roundTripMs: null,
        canary: null,
        error: err?.message ?? String(err),
      };
    }
  }

  private normalizeKey(key: string) {
    return key.replace(/^\/+/, "");
  }

  private sanitizeFileName(name: string | undefined, fallback: string): string {
    const base = (name || fallback || "file")
      .replace(/[\0<>:"|?*]/g, "_")
      .trim();
    return base || "file";
  }

  private buildEntryPath(relPath: string | undefined, fileName: string) {
    // Drop empty, "." and ".." segments so no entry name can escape the
    // buyer's extraction folder (zip-slip) — these values trace back to
    // artist-chosen file/folder names, whatever the caller validates.
    const joined = relPath ? `${relPath}/${fileName}` : fileName;
    const segments = joined
      .split(/[\\/]+/)
      .filter((seg) => seg && seg !== "." && seg !== "..");
    return segments.join("/") || "file";
  }

  private throwIfAborted(signal?: AbortSignal) {
    if (signal?.aborted) throw new Error("aborted");
  }

  private startFetch(
    f: ZipFileDescriptor,
    signal?: AbortSignal,
    requestId?: string
  ): Promise<PrefetchedFile | SkippedFile> {
    const s3Key = this.normalizeKey(f.filePath);
    if (!s3Key) {
      return Promise.resolve({
        kind: "skipped" as const,
        filePath: f.filePath,
        reason: "invalid_key",
      });
    }

    return this.s3
      .send(
        new GetObjectCommand({ Bucket: this.bucket, Key: s3Key }),
        signal ? ({ abortSignal: signal } as any) : undefined
      )
      .then(async (obj) => {
        // Buffer the entire file so archiver writes at memory speed
        const bytes = await obj.Body!.transformToByteArray();
        return {
          kind: "ok" as const,
          descriptor: f,
          s3Key,
          data: Buffer.from(bytes),
          lastModified: obj.LastModified ?? new Date(),
        };
      })
      .catch((err) => {
        if (isNoSuchKey(err)) {
          console.warn("[zip-builder] skipping missing key", {
            requestId,
            s3Key,
          });
          return {
            kind: "skipped" as const,
            filePath: f.filePath,
            reason: "missing",
          };
        }
        throw err;
      });
  }

  /**
   * Resolve an extra entry to bytes. Returns null rather than throwing —
   * a licence that can't be fetched must not cost the buyer their download.
   */
  private async fetchExtraEntry(
    entry: ZipExtraEntry,
    requestId?: string
  ): Promise<Buffer | null> {
    if (typeof entry.text === "string") return Buffer.from(entry.text, "utf8");
    if (!entry.url) return null;

    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), EXTRA_ENTRY_TIMEOUT_MS);
    try {
      const res = await fetch(entry.url, { signal: ac.signal });
      if (!res.ok) {
        console.warn("[zip-builder] extra entry fetch failed", {
          requestId,
          fileName: entry.fileName,
          status: res.status,
        });
        return null;
      }
      const bytes = Buffer.from(await res.arrayBuffer());
      if (bytes.byteLength > EXTRA_ENTRY_MAX_BYTES) {
        console.warn("[zip-builder] extra entry too large, skipping", {
          requestId,
          fileName: entry.fileName,
          bytes: bytes.byteLength,
        });
        return null;
      }
      return bytes;
    } catch (err) {
      console.warn("[zip-builder] extra entry fetch errored", {
        requestId,
        fileName: entry.fileName,
        err,
      });
      return null;
    } finally {
      clearTimeout(timer);
    }
  }

  async buildZip(opts: {
    userId: string;
    zipName: string;
    files: ZipFileDescriptor[];
    extraEntries?: ZipExtraEntry[];
    signal?: AbortSignal;
    requestId?: string;
  }): Promise<ZipBuildResult> {
    // Wrap the actual build in a max-duration race so a hung Wasabi
    // GetObject (or any other indefinite stall) eventually surfaces as a
    // clean error instead of a job that "looks alive" until Bull kills it.
    // We compose a child AbortController so the existing onAbort cleanup
    // path runs when the timer fires.
    const childAc = new AbortController();
    const onParentAbort = () => childAc.abort();
    if (opts.signal) {
      if (opts.signal.aborted) childAc.abort();
      else opts.signal.addEventListener("abort", onParentAbort, { once: true });
    }
    const timeoutHandle = setTimeout(() => {
      console.error("[zip-builder] hard timeout, aborting", {
        requestId: opts.requestId,
        zipName: opts.zipName,
        timeoutMs: MAX_ZIP_DURATION_MS,
      });
      childAc.abort();
    }, MAX_ZIP_DURATION_MS);
    (timeoutHandle as any).unref?.();
    try {
      return await this.buildZipInner({ ...opts, signal: childAc.signal });
    } finally {
      clearTimeout(timeoutHandle);
      if (opts.signal) opts.signal.removeEventListener("abort", onParentAbort);
    }
  }

  private async buildZipInner(opts: {
    userId: string;
    zipName: string;
    files: ZipFileDescriptor[];
    extraEntries?: ZipExtraEntry[];
    signal?: AbortSignal;
    requestId?: string;
  }): Promise<ZipBuildResult> {
    const { userId, zipName, files, extraEntries, signal, requestId } = opts;

    this.throwIfAborted(signal);

    if (!files.length) throw new Error("No files provided to zip");

    const startedAt = Date.now();
    const safeBase = (zipName || "files.zip").replace(/[^a-zA-Z0-9_.-]/g, "_");
    const baseNoExt = safeBase.replace(/\.zip$/i, "");

    const finalKey = `zip/${userId}/${baseNoExt}/current.zip`;
    const contentDisposition = `attachment; filename="${encodeURIComponent(
      `${baseNoExt}.zip`
    )}"`;

    console.log("[zip-builder] start", {
      requestId,
      userId,
      zipName,
      fileCount: files.length,
      finalKey,
    });

    const archive = archiver("zip", { zlib: { level: 0 }, forceZip64: true });

    let bytesWritten = 0;
    let sourceFsBytes = 0;
    let entries = 0;

    archive.on("progress", (p) => {
      sourceFsBytes = p.fs.processedBytes;
      bytesWritten = archive.pointer();
      entries = p.entries.processed;
    });

    archive.on("warning", (w) =>
      console.warn("[zip-builder] warning", { requestId, w })
    );
    // Log only — pipeline(archive, body) already rejects the build on this.
    archive.on("error", (e) => {
      console.error("[zip-builder] archiver error", { requestId, e });
    });

    const body = new PassThrough({ highWaterMark: 16 * 1024 * 1024 });

    const uploader = new Upload({
      client: this.s3,
      params: {
        Bucket: this.bucket,
        Key: finalKey,
        Body: body,
        ContentType: "application/zip",
        ContentDisposition: contentDisposition,
      },
      queueSize: envInt("S3_UPLOAD_QUEUE_SIZE", 8),
      partSize: envInt("S3_UPLOAD_PART_SIZE", 64 * 1024 * 1024),
      leavePartsOnError: false,
    });

    const onAbort = () => {
      try {
        // abort() is async — a rejection here has no awaiter, and an
        // unhandled rejection kills the whole worker.
        uploader.abort().catch(() => {});
      } catch {}
      try {
        archive.destroy(new Error("aborted"));
      } catch {}
      try {
        body.destroy(new Error("aborted"));
      } catch {}
    };
    if (signal) signal.addEventListener("abort", onAbort, { once: true });

    const uploadPromise = uploader.done();
    const pipePromise = pipeline(archive, body);

    const beat = setInterval(() => {
      bytesWritten = archive.pointer();
      console.log("[zip-builder] heartbeat", {
        requestId,
        entries,
        bytesWritten,
        sourceFsBytes,
        elapsedMs: Date.now() - startedAt,
        finalKey,
      });
    }, 10_000).unref();

    const skipped: Array<{ filePath: string; reason: string }> = [];

    try {
      // Sliding-window prefetch: fetch next N files while archiver processes current
      const prefetchQueue: Array<Promise<PrefetchedFile | SkippedFile>> = [];
      let fetchIndex = 0;

      // When the build bails early (abort, S3 error on the awaited fetch),
      // the queued promises are abandoned — and on abort they ALL reject
      // moments later. The noop catch marks those rejections handled so they
      // can't take down the process; the loop still awaits the original
      // promise and sees the real error.
      const queueFetch = (f: ZipFileDescriptor) => {
        const p = this.startFetch(f, signal, requestId);
        p.catch(() => {});
        prefetchQueue.push(p);
      };

      // Prime the queue
      while (
        fetchIndex < files.length &&
        prefetchQueue.length < PREFETCH_CONCURRENCY
      ) {
        queueFetch(files[fetchIndex++]);
      }

      // Consume sequentially, refilling as we go
      while (prefetchQueue.length > 0) {
        this.throwIfAborted(signal);

        const result = await prefetchQueue.shift()!;

        // Refill: start the next fetch
        if (fetchIndex < files.length) {
          queueFetch(files[fetchIndex++]);
        }

        if (result.kind === "skipped") {
          skipped.push({ filePath: result.filePath, reason: result.reason });
          continue;
        }

        const fallbackName = pathPosix.basename(result.s3Key);
        let name = (result.descriptor.fileName || "").trim() || fallbackName;

        const providedExt = pathPosix.extname(name);
        if (!providedExt) {
          const fallbackExt = pathPosix.extname(fallbackName);
          if (fallbackExt) name = `${name}${fallbackExt}`;
        }

        const safeName = this.sanitizeFileName(name, fallbackName);
        const entryName = this.buildEntryPath(result.descriptor.relPath, safeName);

        archive.append(result.data, {
          name: entryName,
          date: result.lastModified,
          store: true,
        });
      }

      // Extras go in last so they sit after the member files. They are also
      // NOT counted in addedFiles/totalFiles: the caller compares those
      // against the file list it sent to decide whether the zip is complete
      // (zipWorkerResultIsComplete), and a licence would skew that.
      let addedExtraEntries = 0;
      for (const entry of extraEntries ?? []) {
        this.throwIfAborted(signal);
        const data = await this.fetchExtraEntry(entry, requestId);
        if (!data) continue;
        archive.append(data, {
          name: this.buildEntryPath(
            undefined,
            this.sanitizeFileName(entry.fileName, "LICENSE.txt")
          ),
          store: true,
        });
        addedExtraEntries += 1;
      }
      if ((extraEntries?.length ?? 0) > addedExtraEntries) {
        console.warn("[zip-builder] extra entries dropped", {
          requestId,
          sent: extraEntries?.length ?? 0,
          added: addedExtraEntries,
        });
      }

      // archive.finalize() returns a promise; previously it was called
      // without await, meaning a synchronous rejection inside finalize()
      // could be lost between archiver and the pipeline await. Tracking
      // it explicitly in Promise.all closes that hole.
      const finalizePromise = archive.finalize();

      await Promise.all([finalizePromise, pipePromise, uploadPromise]);

      bytesWritten = archive.pointer();
      console.log("[zip-builder] done", {
        requestId,
        finalKey,
        elapsedMs: Date.now() - startedAt,
        entries,
        bytesWritten,
        sourceFsBytes,
      });

      if (skipped.length) {
        console.warn("[zip-builder] completed with skipped files", {
          requestId,
          skippedCount: skipped.length,
          skipped: skipped.slice(0, 20),
        });
      }

      return {
        finalKey,
        totalFiles: files.length,
        addedFiles: files.length - skipped.length,
        skippedFiles: skipped,
        addedExtraEntries,
      };
    } catch (err) {
      console.error("[zip-builder] build failed", { requestId, err });
      throw err;
    } finally {
      clearInterval(beat);
      if (signal) signal.removeEventListener("abort", onAbort);
      try {
        archive.destroy();
      } catch {}
      try {
        body.destroy();
      } catch {}
    }
  }
}
