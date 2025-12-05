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

// reuse one handler across all ZipBuilder instances
const httpHandler = new NodeHttpHandler({
  httpsAgent: new HttpsAgent({
    keepAlive: true,
    maxSockets: 200,
    maxFreeSockets: 50,
  }),
});
export interface ZipFileDescriptor {
  filePath: string; // S3 key in Wasabi
  fileName?: string; // optional nice name inside zip
  relPath?: string; // optional folder structure inside zip
}

export interface ZipBuilderConfig {
  endpoint: string;
  region: string;
  bucket: string;
  accessKeyId: string;
  secretAccessKey: string;
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

    this.bucket = cfg.bucket;
    this.s3 = new S3Client({
      region: cfg.region,
      endpoint: cfg.endpoint,
      forcePathStyle: true,
      requestHandler: httpHandler,
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

  /**
   * Build a zip in Wasabi from given files.
   * Returns the final object key (e.g. packs/{userId}/name/current.zip).
   */
  async buildZip(opts: {
    userId: string;
    zipName: string;
    files: ZipFileDescriptor[];
  }): Promise<string> {
    const { userId, zipName, files } = opts;

    if (!files.length) {
      throw new Error("No files provided to zip");
    }

    const startedAt = Date.now();
    const safeBase = (zipName || "files.zip").replace(/[^a-zA-Z0-9_.-]/g, "_");
    const baseNoExt = safeBase.replace(/\.zip$/i, "");
    const stamp = Date.now();

    const tmpKey = `zip/${userId}/${baseNoExt}/building/${stamp}.zip`;
    const finalKey = `zip/${userId}/${baseNoExt}/current.zip`;

    console.log("[zip-builder] start", {
      userId,
      zipName,
      fileCount: files.length,
      tmpKey,
      finalKey,
    });

    // Setup archiver (store only, no compression)
    const archive = archiver("zip", {
      zlib: { level: 0 },
      forceZip64: true,
    });

    let bytesWritten = 0;
    let entries = 0;
    archive.on("progress", (p) => {
      bytesWritten = p.fs.processedBytes;
      entries = p.entries.processed;
    });
    archive.on("warning", (w) => console.warn("[zip-builder] warning", w));
    archive.on("error", (e) => {
      console.error("[zip-builder] archiver error", e);
      archive.destroy(e);
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
      queueSize: 8,
      partSize: 16 * 1024 * 1024,
      leavePartsOnError: false,
    });

    const uploadPromise = uploader.done();
    const pipePromise = pipeline(archive, body);

    let zipBytes = 0;
    body.on("data", (chunk) => {
      zipBytes += chunk.length;
    });

    const beat = setInterval(() => {
      console.log("[zip-builder] heartbeat", {
        entries,
        zipBytes,
        elapsedMs: Date.now() - startedAt,
        finalKey,
      });
    }, 5000).unref();

    try {
      // Append files sequentially
      for (const f of files) {
        const s3Key = this.normalizeKey(f.filePath);
        if (!s3Key) {
          throw new Error(`Invalid filePath: "${f.filePath}"`);
        }

        const obj = await this.s3.send(
          new GetObjectCommand({
            Bucket: this.bucket,
            Key: s3Key,
          })
        );

        const stream = obj.Body as any;
        const lastModified = obj.LastModified ?? new Date();

        const fallbackName = pathPosix.basename(s3Key);
        const safeName = this.sanitizeFileName(f.fileName, fallbackName);
        const entryName = this.buildEntryPath(f.relPath, safeName);

        console.log("[zip-builder] adding", { s3Key, entryName });

        stream.on?.("error", (e: any) => archive.emit("error", e));

        archive.append(stream, {
          name: entryName,
          date: lastModified,
          store: true,
        });
      }

      // Finalize & wait for both sides
      archive.finalize();

      const [_, uploadResult] = await Promise.all([pipePromise, uploadPromise]);

      console.log("[zip-builder] upload complete", {
        etag: (uploadResult as any)?.ETag,
        tmpKey,
      });

      // Promote tmp → final
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
        })
      );

      await this.s3.send(
        new DeleteObjectCommand({
          Bucket: this.bucket,
          Key: tmpKey,
        })
      );

      console.log("[zip-builder] done", {
        finalKey,
        elapsedMs: Date.now() - startedAt,
        entries,
        bytesWritten,
      });

      return finalKey;
    } catch (err) {
      console.error("[zip-builder] build failed", err);
      // Let caller handle error
      throw err;
    } finally {
      clearInterval(beat);
      try {
        archive.destroy();
      } catch {}
      try {
        body.destroy();
      } catch {}
    }
  }
}
