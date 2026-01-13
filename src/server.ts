// src/server.ts
import "dotenv/config";
import { createServer, IncomingMessage, ServerResponse } from "http";
import { timingSafeEqual, randomUUID } from "crypto";
import { ZipBuilder, ZipFileDescriptor } from "./storage/zip-builder";

const PORT = Number(process.env.PORT || 4005);
const API_KEY = process.env.ZIP_SERVICE_API_KEY || "";
const MAX_BODY_BYTES = +(process.env.MAX_BODY_BYTES || 100_000_000); // 100MB
const MAX_FILES = +(process.env.MAX_FILES || 5_000);
if (!API_KEY) {
  console.warn(
    "[zip-service] WARNING: ZIP_SERVICE_API_KEY is not set. Service will accept all requests."
  );
}

const zipBuilder = new ZipBuilder({
  endpoint: process.env.WASABI_ENDPOINT!,
  region: process.env.WASABI_REGION!,
  bucket: process.env.WASABI_BUCKET_NAME!,
  accessKeyId: process.env.STORAGE_ACCESS_KEY_ID!,
  secretAccessKey: process.env.STORAGE_SECRET_ACCESS_KEY!,
});

type ZipRequestBody = {
  userId: string;
  zipName: string;
  files: ZipFileDescriptor[];
};

const MAX_CONCURRENT_ZIPS = +(process.env.MAX_CONCURRENT_ZIPS || 2);

let inFlight = 0;
const waiters: Array<() => void> = [];

async function withZipSlot<T>(fn: () => Promise<T>, signal?: AbortSignal) {
  if (signal?.aborted) throw new Error("aborted");

  if (inFlight >= MAX_CONCURRENT_ZIPS) {
    await new Promise<void>((resolve, reject) => {
      const onAbort = () => {
        cleanup();
        reject(new Error("aborted"));
      };
      const cleanup = () => {
        if (signal) signal.removeEventListener("abort", onAbort);
      };

      if (signal) signal.addEventListener("abort", onAbort, { once: true });
      waiters.push(() => {
        cleanup();
        resolve();
      });
    });
  }

  inFlight++;
  try {
    if (signal?.aborted) throw new Error("aborted");
    return await fn();
  } finally {
    inFlight--;
    waiters.shift()?.();
  }
}

function readJsonBody(req: IncomingMessage): Promise<any> {
  return new Promise((resolve, reject) => {
    let total = 0;
    const chunks: Buffer[] = [];

    req.on("data", (chunk: Buffer) => {
      total += chunk.length;

      if (total > MAX_BODY_BYTES) {
        // stop reading immediately
        req.destroy();
        reject(new Error("payload_too_large"));
        return;
      }

      chunks.push(chunk);
    });

    req.on("end", () => {
      try {
        const raw = Buffer.concat(chunks).toString("utf8");
        resolve(raw ? JSON.parse(raw) : {});
      } catch (err) {
        reject(err);
      }
    });

    req.on("error", reject);
  });
}

function unauthorized(res: ServerResponse) {
  res.statusCode = 401;
  res.setHeader("Content-Type", "application/json");
  res.end(JSON.stringify({ error: "Unauthorized" }));
}

function checkApiKey(req: IncomingMessage): boolean {
  if (!API_KEY) return true;

  const headerKey =
    (req.headers["x-zip-service-key"] as string | undefined) ||
    (req.headers["x-api-key"] as string | undefined) ||
    "";

  const a = Buffer.from(headerKey);
  const b = Buffer.from(API_KEY);

  return a.length === b.length && timingSafeEqual(a, b);
}

function badRequest(res: ServerResponse, error: string) {
  res.statusCode = 400;
  res.setHeader("Content-Type", "application/json");
  res.end(JSON.stringify({ success: false, error }));
}

async function handleRequest(req: IncomingMessage, res: ServerResponse) {
  if (req.method === "GET" && req.url === "/health") {
    res.statusCode = 200;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ status: "ok" }));
    return;
  }

  if (!checkApiKey(req)) return unauthorized(res);

  if (req.method === "POST" && req.url === "/zip") {
    const requestId = req.headers["x-request-id"]?.toString() || randomUUID();
    if (!req.headers["content-type"]?.includes("application/json"))
      return badRequest(res, "content_type_must_be_json");
const abort = new AbortController();

// best signals:
req.on("aborted", () => abort.abort()); // client aborted request
res.on("close", () => abort.abort());   // client disconnected before response finished

    try {
      const body = (await readJsonBody(req)) as ZipRequestBody;

      if (!body.userId || !body.zipName || !Array.isArray(body.files)) {
        return badRequest(res, "invalid_payload");
      }
      if (body.files.length === 0) return badRequest(res, "no_files");
      if (body.files.length > MAX_FILES)
        return badRequest(res, "too_many_files");

      console.log("[zip-service] /zip request", {
        requestId,
        userId: body.userId,
        zipName: body.zipName,
        fileCount: body.files.length,
      });

      // (Step 2/3 will improve how buildZip runs)
      const finalKey = await withZipSlot(
        () =>
          zipBuilder.buildZip({
            userId: body.userId,
            zipName: body.zipName,
            files: body.files,
            requestId,
            signal: abort.signal,
          } as any),
        abort.signal
      );
      res.statusCode = 200;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ success: true, requestId, finalKey }));
    } catch (err: any) {
      const msg = err?.message ?? "Internal error";
      const status = msg === "payload_too_large" ? 413 : 500;
      console.error("[zip-service] error", { msg, err });
      res.statusCode = status;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ success: false, error: msg }));
    }
    return;
  }

  res.statusCode = 404;
  res.setHeader("Content-Type", "application/json");
  res.end(JSON.stringify({ error: "Not found" }));
}

const server = createServer(handleRequest);
server.keepAliveTimeout = 70_000;
server.headersTimeout = 75_000;
server.requestTimeout = 0; // let your own job timeout logic control this

server.listen(PORT, () => {
  console.log(`[zip-service] listening on http://localhost:${PORT}`);
});
