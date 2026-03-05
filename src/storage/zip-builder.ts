// zip-builder.ts
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import * as archiver from "archiver";
import { PassThrough } from "stream";
import { pipeline } from "stream/promises";
import { posix as pathPosix } from "node:path";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import { Agent as HttpsAgent } from "https";

const CONNECT_TIMEOUT_MS = +(process.env.S3_CONNECT_TIMEOUT_MS || 5_000);
const SOCKET_TIMEOUT_MS = +(process.env.S3_SOCKET_TIMEOUT_MS || 60_000);
const PREFETCH_CONCURRENCY = +(process.env.S3_PREFETCH_CONCURRENCY || 6);

const httpHandler = new NodeHttpHandler({
  httpsAgent: new HttpsAgent({
    keepAlive: true,
    keepAliveMsecs: 10_000,
    maxSockets: +(process.env.S3_MAX_SOCKETS || 512),
    maxFreeSockets: +(process.env.S3_MAX_FREE_SOCKETS || 128),
  }),
  connectionTimeout: CONNECT_TIMEOUT_MS,
  socketTimeout: SOCKET_TIMEOUT_MS,
});

export interface ZipFileDescriptor {
  filePath: string;
  fileName?: string;
  relPath?: string;
}

export interface ZipBuildResult {
  finalKey: string;
  totalFiles: number;
  addedFiles: number;
  skippedFiles: Array<{ filePath: string; reason: string }>;
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

export class ZipBuilder {
  private s3: S3Client;
  private bucket: string;

  constructor(cfg: ZipBuilderConfig) {
    if (!cfg.accessKeyId || !cfg.secretAccessKey) {
      throw new Error(
        "ZipBuilder: missing accessKeyId/secretAccessKey – check env vars"
      );
    }

    const MAX_ATTEMPTS = +(process.env.S3_MAX_ATTEMPTS || 4);

    this.bucket = cfg.bucket;
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
    const cleanName = fileName.replace(/^\/+/, "");
    if (!relPath) return cleanName;

    const cleanRel = relPath.replace(/^\/+/, "").replace(/\/+$/, "");
    return `${cleanRel}/${cleanName}`;
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

  async buildZip(opts: {
    userId: string;
    zipName: string;
    files: ZipFileDescriptor[];
    signal?: AbortSignal;
    requestId?: string;
  }): Promise<ZipBuildResult> {
    const { userId, zipName, files, signal, requestId } = opts;

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
    let entries = 0;

    archive.on("progress", (p) => {
      bytesWritten = p.fs.processedBytes;
      entries = p.entries.processed;
    });

    archive.on("warning", (w) =>
      console.warn("[zip-builder] warning", { requestId, w })
    );
    let archiveErr: any = null;

    archive.on("error", (e) => {
      archiveErr = e;
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
      queueSize: +(process.env.S3_UPLOAD_QUEUE_SIZE || 8),
      partSize: +(process.env.S3_UPLOAD_PART_SIZE || 64 * 1024 * 1024),
      leavePartsOnError: false,
    });

    const onAbort = () => {
      try {
        uploader.abort();
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
      console.log("[zip-builder] heartbeat", {
        requestId,
        entries,
        bytesWritten,
        elapsedMs: Date.now() - startedAt,
        finalKey,
      });
    }, 10_000).unref();

    const skipped: Array<{ filePath: string; reason: string }> = [];

    try {
      // Sliding-window prefetch: fetch next N files while archiver processes current
      const prefetchQueue: Array<Promise<PrefetchedFile | SkippedFile>> = [];
      let fetchIndex = 0;

      // Prime the queue
      while (
        fetchIndex < files.length &&
        prefetchQueue.length < PREFETCH_CONCURRENCY
      ) {
        prefetchQueue.push(
          this.startFetch(files[fetchIndex++], signal, requestId)
        );
      }

      // Consume sequentially, refilling as we go
      while (prefetchQueue.length > 0) {
        this.throwIfAborted(signal);

        const result = await prefetchQueue.shift()!;

        // Refill: start the next fetch
        if (fetchIndex < files.length) {
          prefetchQueue.push(
            this.startFetch(files[fetchIndex++], signal, requestId)
          );
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

      archive.finalize();

      await Promise.all([pipePromise, uploadPromise]);

      console.log("[zip-builder] done", {
        requestId,
        finalKey,
        elapsedMs: Date.now() - startedAt,
        entries,
        bytesWritten,
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
