// src/server.ts
import "dotenv/config";
import { createServer, IncomingMessage, ServerResponse } from "http";
import { timingSafeEqual, randomUUID } from "crypto";
import {
  ZipBuilder,
  ZipFileDescriptor,
  ZipBuildResult,
  ZipExtraEntry,
  StorageProbeResult,
} from "./storage/zip-builder";

const PORT = Number(process.env.PORT || 4005);
const VERSION = "1.0.4";
const API_KEY = process.env.ZIP_SERVICE_API_KEY || "";
const MAX_BODY_BYTES = +(process.env.MAX_BODY_BYTES || 1_000_000_000); // 1GB
const MAX_FILES = +(process.env.MAX_FILES || 5_000);
if (!API_KEY) {
  console.warn(
    "[zip-service] WARNING: ZIP_SERVICE_API_KEY is not set. Service will accept all requests.",
  );
}

const zipBuilder = new ZipBuilder({
  endpoint: process.env.WASABI_ENDPOINT!,
  region: process.env.WASABI_REGION!,
  bucket: process.env.WASABI_BUCKET_NAME!,
  accessKeyId: process.env.STORAGE_ACCESS_KEY_ID!,
  secretAccessKey: process.env.STORAGE_SECRET_ACCESS_KEY!,
});

// The legacy Wasabi bucket predates the R2 cutover; a worker pointed there
// silently misses every object uploaded since. It still boots (old packs DO
// resolve there), but /health reports degraded so monitoring screams.
const LEGACY_ENDPOINT = /wasabisys\.com/i.test(
  process.env.WASABI_ENDPOINT || ""
);

// ---------------------------------------------------------------------------
// Error tracking: a ring buffer of recent errors plus lifetime counters,
// surfaced via /health (counts) and /errors (details). In-memory on purpose —
// this is for "what went wrong lately", not an audit log; container restarts
// reset it, and the backend keeps the durable record in pack.zipJson.
// ---------------------------------------------------------------------------
type ServiceError = {
  at: string;
  scope:
    | "build_failed"
    | "partial_zip"
    | "extras_dropped"
    | "storage_probe"
    | "bad_request";
  message: string;
  detail?: Record<string, unknown>;
};

const MAX_TRACKED_ERRORS = +(process.env.MAX_TRACKED_ERRORS || 100);
const recentErrors: ServiceError[] = [];

const counters = {
  startedAt: new Date().toISOString(),
  builds: 0,
  buildFailures: 0,
  partialBuilds: 0,
  skippedFilesTotal: 0,
  unauthorizedRequests: 0,
  lastBuildAt: null as string | null,
  lastBuildOk: null as boolean | null,
};

function recordError(
  scope: ServiceError["scope"],
  message: string,
  detail?: Record<string, unknown>
) {
  recentErrors.unshift({ at: new Date().toISOString(), scope, message, detail });
  if (recentErrors.length > MAX_TRACKED_ERRORS) recentErrors.length = MAX_TRACKED_ERRORS;
  console.error(`[zip-service] ${scope}: ${message}`, detail ?? {});
}

// ---------------------------------------------------------------------------
// Storage probe: at boot and on an interval, prove the configured bucket is
// writable/readable — and, when STORAGE_CANARY_KEY is set to a key the
// backend recently wrote, that it is the SAME bucket the backend uses.
// ---------------------------------------------------------------------------
const PROBE_INTERVAL_MS = +(process.env.STORAGE_PROBE_INTERVAL_MS || 5 * 60_000);
let lastProbe: StorageProbeResult | null = null;

async function runStorageProbe() {
  const result = await zipBuilder.probeStorage(process.env.STORAGE_CANARY_KEY);
  const wasOk = lastProbe?.ok;
  lastProbe = result;
  if (!result.ok && wasOk !== false) {
    recordError("storage_probe", result.error ?? "storage probe failed", {
      bucket: zipBuilder.bucketName,
      endpoint: zipBuilder.endpointHost,
      canary: result.canary,
    });
  }
  if (result.ok && wasOk === false) {
    console.log("[zip-service] storage probe recovered", { at: result.at });
  }
}

function healthPayload() {
  const storageOk = (lastProbe?.ok ?? true) && !LEGACY_ENDPOINT;
  return {
    status: storageOk ? "ok" : "degraded",
    version: VERSION,
    storage: {
      ok: storageOk,
      endpoint: zipBuilder.endpointHost,
      bucket: zipBuilder.bucketName,
      legacyEndpoint: LEGACY_ENDPOINT,
      lastProbe,
    },
    zips: {
      inFlight,
      queued: waiters.length,
      maxConcurrent: MAX_CONCURRENT_ZIPS,
      ...counters,
    },
    recentErrorCount: recentErrors.length,
  };
}

type ZipRequestBody = {
  userId: string;
  zipName: string;
  files: ZipFileDescriptor[];
  /** Non-member entries — today, the pack licence. See ZipExtraEntry. */
  extraEntries?: ZipExtraEntry[];
};

// A handful at most; the field exists for licences, not for smuggling in a
// second file list that bypasses MAX_FILES.
const MAX_EXTRA_ENTRIES = +(process.env.MAX_EXTRA_ENTRIES || 4);

/**
 * Keep only well-formed entries. A malformed one is dropped rather than
 * failing the request — the zip itself is what the buyer is waiting on.
 */
function sanitizeExtraEntries(raw: unknown): ZipExtraEntry[] {
  if (!Array.isArray(raw)) return [];
  return raw
    .filter(
      (e): e is ZipExtraEntry =>
        !!e &&
        typeof e === "object" &&
        typeof (e as any).fileName === "string" &&
        !!(e as any).fileName &&
        (typeof (e as any).text === "string" ||
          typeof (e as any).url === "string")
    )
    .slice(0, MAX_EXTRA_ENTRIES);
}

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
  counters.unauthorizedRequests += 1;
  res.statusCode = 401;
  res.setHeader("Content-Type", "application/json");
  res.end(JSON.stringify({ error: "Unauthorized", version: VERSION }));
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
    const payload = healthPayload();
    // 503 on degraded storage so a dumb uptime monitor keying off the status
    // code catches a misconfigured bucket, not just a dead process.
    res.statusCode = payload.status === "ok" ? 200 : 503;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify(payload));
    return;
  }

  if (!checkApiKey(req)) return unauthorized(res);

  if (req.method === "GET" && req.url === "/errors") {
    res.statusCode = 200;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ counters, errors: recentErrors }));
    return;
  }

  if (req.method === "POST" && req.url === "/zip") {
    const requestId = req.headers["x-request-id"]?.toString() || randomUUID();
    if (!req.headers["content-type"]?.includes("application/json"))
      return badRequest(res, "content_type_must_be_json");
    const abort = new AbortController();

    // best signals:
    req.on("aborted", () => abort.abort()); // client aborted request
    res.on("close", () => abort.abort()); // client disconnected before response finished

    try {
      const body = (await readJsonBody(req)) as ZipRequestBody;

      if (!body.userId || !body.zipName || !Array.isArray(body.files)) {
        return badRequest(res, "invalid_payload");
      }
      if (body.files.length === 0) return badRequest(res, "no_files");
      if (body.files.length > MAX_FILES)
        return badRequest(res, "too_many_files");

      const extraEntries = sanitizeExtraEntries(body.extraEntries);

      console.log("[zip-service] /zip request", {
        requestId,
        userId: body.userId,
        zipName: body.zipName,
        fileCount: body.files.length,
        extraEntryCount: extraEntries.length,
      });

      const result = await withZipSlot(
        () =>
          zipBuilder.buildZip({
            userId: body.userId,
            zipName: body.zipName,
            files: body.files,
            extraEntries,
            requestId,
            signal: abort.signal,
          }),
        abort.signal,
      );

      counters.builds += 1;
      counters.lastBuildAt = new Date().toISOString();
      counters.lastBuildOk = true;
      if (result.skippedFiles.length > 0) {
        counters.partialBuilds += 1;
        counters.skippedFilesTotal += result.skippedFiles.length;
        recordError(
          "partial_zip",
          `${result.skippedFiles.length}/${result.totalFiles} files skipped`,
          {
            requestId,
            zipName: body.zipName,
            skipped: result.skippedFiles.slice(0, 10),
          },
        );
      }
      if (extraEntries.length > result.addedExtraEntries) {
        recordError(
          "extras_dropped",
          `${extraEntries.length - result.addedExtraEntries} extra entries (licence) dropped`,
          { requestId, zipName: body.zipName },
        );
      }

      res.statusCode = 200;
      res.setHeader("Content-Type", "application/json");
      res.end(
        JSON.stringify({
          success: true,
          requestId,
          finalKey: result.finalKey,
          totalFiles: result.totalFiles,
          addedFiles: result.addedFiles,
          skippedFiles: result.skippedFiles,
          // The caller records the licence in its manifest only when this
          // confirms it shipped — absent means a worker that predates extras.
          addedExtraEntries: result.addedExtraEntries,
        }),
      );
    } catch (err: any) {
      const msg = err?.message ?? "Internal error";
      const status = msg === "payload_too_large" ? 413 : 500;
      counters.buildFailures += 1;
      counters.lastBuildAt = new Date().toISOString();
      counters.lastBuildOk = false;
      recordError("build_failed", msg, { requestId });
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

const version = () => {
  try {
    const pkg = require("../package.json");
    return pkg.version || "unknown";
  } catch {
    return "unknown";
  }
};

const server = createServer(handleRequest);
server.keepAliveTimeout = 70_000;
server.headersTimeout = 75_000;
server.requestTimeout = 0; // let your own job timeout logic control this

server.listen(PORT, () => {
  console.log(`[zip-service] listening on http://localhost:${PORT}`);
  console.log("[zip-service] max concurrent zips:", MAX_CONCURRENT_ZIPS);
  console.log(`[zip-service] version: ${VERSION}`);
  console.log("[zip-service] storage:", {
    endpoint: zipBuilder.endpointHost,
    bucket: zipBuilder.bucketName,
    legacyEndpoint: LEGACY_ENDPOINT,
  });
  if (LEGACY_ENDPOINT) {
    recordError(
      "storage_probe",
      "configured against legacy Wasabi endpoint — objects uploaded since the R2 cutover will be missing from every zip",
      { endpoint: zipBuilder.endpointHost, bucket: zipBuilder.bucketName },
    );
  }
  void runStorageProbe();
  setInterval(() => void runStorageProbe(), PROBE_INTERVAL_MS).unref();
});
