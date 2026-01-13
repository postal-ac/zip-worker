// zip-builder.ts
import {
  S3Client,
  GetObjectCommand,
  CopyObjectCommand,
  DeleteObjectCommand,
} from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import * as archiver from "archiver";
import { PassThrough } from "stream";
import { pipeline } from "node:stream/promises";
import { posix as pathPosix } from "node:path";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import { Agent as HttpsAgent } from "https";

const CONNECT_TIMEOUT_MS = +(process.env.S3_CONNECT_TIMEOUT_MS || 5_000);
const SOCKET_TIMEOUT_MS = +(process.env.S3_SOCKET_TIMEOUT_MS || 60_000);

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

export interface ZipBuilderConfig {
  endpoint: string;
  region: string;
  bucket: string;
  accessKeyId: string;
  secretAccessKey: string;
}

function safeStream(input: any, onErr: (e: any) => void) {
  const out = new PassThrough();
  input.on("error", (e: any) => {
    onErr(e);
    out.end(); // finish this zip entry gracefully
  });
  input.pipe(out);
  return out;
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
      // (Step 2 will add retries config)
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

  async buildZip(opts: {
    userId: string;
    zipName: string;
    files: ZipFileDescriptor[];
    signal?: AbortSignal; // ✅ NEW
    requestId?: string; // optional for logs
  }): Promise<string> {
    const { userId, zipName, files, signal, requestId } = opts;

    this.throwIfAborted(signal);

    if (!files.length) throw new Error("No files provided to zip");

    const startedAt = Date.now();
    const safeBase = (zipName || "files.zip").replace(/[^a-zA-Z0-9_.-]/g, "_");
    const baseNoExt = safeBase.replace(/\.zip$/i, "");
    const stamp = Date.now();

    const tmpKey = `zip/${userId}/${baseNoExt}/building/${stamp}.zip`;
    const finalKey = `zip/${userId}/${baseNoExt}/current.zip`;

    console.log("[zip-builder] start", {
      requestId,
      userId,
      zipName,
      fileCount: files.length,
      tmpKey,
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
        Key: tmpKey,
        Body: body,
        ContentType: "application/zip",
      },
      queueSize: +(process.env.S3_UPLOAD_QUEUE_SIZE || 4), // was 8
      partSize: +(process.env.S3_UPLOAD_PART_SIZE || 64 * 1024 * 1024), // was 16MB
      leavePartsOnError: false,
    });

    // ✅ Abort behavior: stop upload + streams ASAP
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

    let zipBytes = 0;
    body.on("data", (chunk) => (zipBytes += chunk.length));

    const beat = setInterval(() => {
      console.log("[zip-builder] heartbeat", {
        requestId,
        entries,
        zipBytes,
        elapsedMs: Date.now() - startedAt,
        finalKey,
      });
    }, 5000).unref();

    const skipped: Array<{ filePath: string; reason: string }> = [];

    try {
      for (const f of files) {
        this.throwIfAborted(signal);

        const s3Key = this.normalizeKey(f.filePath);
        if (!s3Key) {
          skipped.push({ filePath: f.filePath, reason: "invalid_key" });
          continue;
        }

        let obj: any;
        try {
          obj = await this.s3.send(
            new GetObjectCommand({ Bucket: this.bucket, Key: s3Key }),
            signal ? ({ abortSignal: signal } as any) : undefined
          );
        } catch (err: any) {
          if (isNoSuchKey(err)) {
            console.warn("[zip-builder] skipping missing key", {
              requestId,
              s3Key,
            });
            skipped.push({ filePath: f.filePath, reason: "missing" });
            continue;
          }
          throw err; // real error -> fail the zip
        }

        const raw = obj.Body as any;
        const stream = safeStream(raw, (e) => {
          console.warn("[zip-builder] stream error (skipping remainder)", {
            requestId,
            s3Key,
            err: e?.message ?? e,
          });
          skipped.push({ filePath: f.filePath, reason: "stream_error" });
        });

        const lastModified = obj.LastModified ?? new Date();

        const fallbackName = pathPosix.basename(s3Key);
        const safeName = this.sanitizeFileName(f.fileName, fallbackName);
        const entryName = this.buildEntryPath(f.relPath, safeName);

        console.log("[zip-builder] adding", { requestId, s3Key, entryName });

        stream?.on?.("error", (e: any) => {
          console.warn(
            "[zip-builder] stream error, file will be incomplete/skipped",
            {
              requestId,
              s3Key,
              err: e?.message ?? e,
            }
          );
        });

        archive.append(stream, {
          name: entryName,
          date: lastModified,
          store: true,
        });
      }

      archive.finalize();

      const [, uploadResult] = await Promise.all([pipePromise, uploadPromise]);

      console.log("[zip-builder] upload complete", {
        requestId,
        etag: (uploadResult as any)?.ETag,
        tmpKey,
      });

      const copySource = `${this.bucket}/${encodeURIComponent(
        tmpKey.replace(/^\/+/, "")
      ).replace(/%2F/g, "/")}`;

      await this.s3.send(
        new CopyObjectCommand({
          Bucket: this.bucket,
          CopySource: copySource,
          Key: finalKey,
          ContentType: "application/zip",
          ContentDisposition: `attachment; filename="${encodeURIComponent(
            `${baseNoExt}.zip`
          )}"`,
          MetadataDirective: "REPLACE",
        }),
        signal ? ({ abortSignal: signal } as any) : undefined
      );

      await this.s3.send(
        new DeleteObjectCommand({ Bucket: this.bucket, Key: tmpKey }),
        signal ? ({ abortSignal: signal } as any) : undefined
      );

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
          skipped: skipped.slice(0, 20), // don’t spam
        });
      }

      return finalKey;
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
