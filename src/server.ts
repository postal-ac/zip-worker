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
import { envInt } from "./env";

// One tag for every log line, matching the folder/container name — so one
// grep finds this service's output during an incident.
type LogCtx = Record<string, unknown>;
const log = {
  info: (msg: string, ctx?: LogCtx) =>
    ctx === undefined
      ? console.log(`[zip-worker] ${msg}`)
      : console.log(`[zip-worker] ${msg}`, ctx),
  warn: (msg: string, ctx?: LogCtx) =>
    ctx === undefined
      ? console.warn(`[zip-worker] ${msg}`)
      : console.warn(`[zip-worker] ${msg}`, ctx),
  error: (msg: string, ctx?: LogCtx) =>
    ctx === undefined
      ? console.error(`[zip-worker] ${msg}`)
      : console.error(`[zip-worker] ${msg}`, ctx),
};

// ---------------------------------------------------------------------------
// Config. Required vars are collected and reported together at boot — a
// worker that starts on half a config fails at request time with a
// confusing SDK error instead (the compose-mapping incident).
// ---------------------------------------------------------------------------
const missingEnv: string[] = [];
function requiredEnv(name: string): string {
  const v = process.env[name];
  if (!v) missingEnv.push(name);
  return v ?? "";
}

const PORT = envInt("PORT", 4005);
// Single-sourced from package.json: /health's version field is how a stale
// or misconfigured host gets unmasked — it must never drift from the build.
const VERSION: string = require("../package.json").version ?? "unknown";
const API_KEY = process.env.ZIP_SERVICE_API_KEY || "";
// The body is a JSON list of file descriptors — a few MB even at MAX_FILES.
const MAX_BODY_BYTES = envInt("MAX_BODY_BYTES", 25_000_000);
const MAX_FILES = envInt("MAX_FILES", 5_000);
if (!API_KEY) {
  log.warn(
    "WARNING: ZIP_SERVICE_API_KEY is not set. Service will accept all requests.",
  );
}

const storageConfig = {
  endpoint: requiredEnv("STORAGE_ENDPOINT"),
  region: requiredEnv("STORAGE_REGION"),
  bucket: requiredEnv("STORAGE_BUCKET_NAME"),
  accessKeyId: requiredEnv("STORAGE_ACCESS_KEY_ID"),
  secretAccessKey: requiredEnv("STORAGE_SECRET_ACCESS_KEY"),
};
if (missingEnv.length > 0) {
  log.error(`missing required env vars: ${missingEnv.join(", ")}`);
  process.exit(1);
}

const zipBuilder = new ZipBuilder(storageConfig);

// ---------------------------------------------------------------------------
// Error tracking: a bounded most-recent-first list of errors plus lifetime counters,
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

const MAX_TRACKED_ERRORS = envInt("MAX_TRACKED_ERRORS", 100);
const recentErrors: ServiceError[] = [];

const counters = {
  startedAt: new Date().toISOString(),
  buildsSucceeded: 0,
  buildFailures: 0,
  abortedBuilds: 0,
  queueRejected: 0,
  badRequests: 0,
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
  log.error(`${scope}: ${message}`, detail ?? {});
}

// ---------------------------------------------------------------------------
// Storage probe: at boot and on an interval, prove the configured bucket is
// writable/readable — and, when STORAGE_CANARY_KEY is set to a key the
// backend recently wrote, that it is the SAME bucket the backend uses.
// ---------------------------------------------------------------------------
const PROBE_INTERVAL_MS = envInt("STORAGE_PROBE_INTERVAL_MS", 5 * 60_000);
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
    log.info("storage probe recovered", { at: result.at });
  }
}

function healthPayload() {
  const storageOk = (lastProbe?.ok ?? true);
  return {
    status: shuttingDown ? "draining" : storageOk ? "ok" : "degraded",
    version: VERSION,
    storage: {
      ok: storageOk,
      endpoint: zipBuilder.endpointHost,
      bucket: zipBuilder.bucketName,
      lastProbe,
    },
    zips: {
      inFlight,
      queued: waiters.length,
      maxConcurrent: MAX_CONCURRENT_ZIPS,
      maxQueueDepth: MAX_QUEUE_DEPTH,
      ...counters,
    },
    recentErrorCount: recentErrors.length,
  };
}

/** An error with a definite HTTP status, so the catch block never has to
 * dispatch on message strings. Anything else surfaces as a 500. */
class HttpError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

/** Thrown when the wait queue is full — shed load, not a build failure. */
class QueueFullError extends HttpError {
  constructor() {
    super(503, "queue_full");
  }
}

function sendJson(res: ServerResponse, status: number, body: unknown) {
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json");
  res.end(JSON.stringify(body));
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
const MAX_EXTRA_ENTRIES = envInt("MAX_EXTRA_ENTRIES", 4);

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
        !hasDotDotSegment((e as any).fileName) &&
        (typeof (e as any).text === "string" ||
          typeof (e as any).url === "string")
    )
    .slice(0, MAX_EXTRA_ENTRIES);
}

/** True when any path segment is "..": the zip-slip building block. */
function hasDotDotSegment(p: string): boolean {
  return p.split(/[\\/]+/).some((seg) => seg === "..");
}

/**
 * Validate at the boundary and fail loudly with the exact field: a malformed
 * value from the backend means FE/BE drift, and a ".." segment is a zip-slip
 * attempt — neither should be silently repaired here. (The builder ALSO
 * strips ".." segments as a second layer; see buildEntryPath.)
 *
 * userId is only banned from containing path separators / dot-segments —
 * not forced to a strict alphabet — so an exotic but harmless ID format
 * never breaks downloads; separators are what corrupt the zip/{userId}/
 * key layout.
 */
function parseZipRequest(raw: any): ZipRequestBody {
  if (!raw || typeof raw !== "object")
    throw new HttpError(400, "invalid_payload");

  const { userId, zipName, files } = raw;

  if (typeof userId !== "string" || !userId.trim())
    throw new HttpError(400, "userId must be a non-empty string");
  if (/[\\/]/.test(userId) || userId === ".." || userId === ".")
    throw new HttpError(400, "userId must not contain path separators");
  if (typeof zipName !== "string" || !zipName.trim())
    throw new HttpError(400, "zipName must be a non-empty string");

  if (!Array.isArray(files)) throw new HttpError(400, "files must be an array");
  if (files.length === 0) throw new HttpError(400, "no_files");
  if (files.length > MAX_FILES) throw new HttpError(400, "too_many_files");

  files.forEach((f, i) => {
    if (!f || typeof f !== "object")
      throw new HttpError(400, `files[${i}] is not an object`);
    if (typeof f.filePath !== "string" || !f.filePath.trim())
      throw new HttpError(400, `files[${i}].filePath must be a non-empty string`);
    for (const field of ["fileName", "relPath"] as const) {
      if (f[field] !== undefined && typeof f[field] !== "string")
        throw new HttpError(400, `files[${i}].${field} must be a string`);
    }
    for (const field of ["filePath", "fileName", "relPath"] as const) {
      const v = f[field];
      if (typeof v === "string" && hasDotDotSegment(v))
        throw new HttpError(400, `files[${i}].${field} contains a ".." path segment`);
    }
  });

  return {
    userId,
    zipName,
    files: files as ZipFileDescriptor[],
    extraEntries: sanitizeExtraEntries(raw.extraEntries),
  };
}

const MAX_CONCURRENT_ZIPS = envInt("MAX_CONCURRENT_ZIPS", 2);
// Callers give up after 15–60s; a deeper queue than this is just zombie
// builds nobody will download. Full queue → 503, the backend retries.
const MAX_QUEUE_DEPTH = envInt("MAX_QUEUE_DEPTH", 8);

let inFlight = 0;
const waiters: Array<() => void> = [];

// Shutdown drain state: while shuttingDown, new /zip requests get an
// immediate retryable 503 and /health reports "draining". activeBuilds
// lets the drain deadline abort stragglers through the builder's own
// cleanup path (which aborts the multipart upload — no orphaned parts).
let shuttingDown = false;
const activeBuilds = new Set<AbortController>();

async function withZipSlot<T>(fn: () => Promise<T>, signal?: AbortSignal) {
  if (signal?.aborted) throw new Error("aborted");

  if (inFlight >= MAX_CONCURRENT_ZIPS) {
    if (waiters.length >= MAX_QUEUE_DEPTH) throw new QueueFullError();

    await new Promise<void>((resolve, reject) => {
      const wake = () => {
        cleanup();
        resolve();
      };
      const onAbort = () => {
        cleanup();
        // Take our wake-up out of the line: a dead entry left behind would
        // swallow the single wake-up a finished build hands to the queue,
        // stalling the live request behind it.
        const i = waiters.indexOf(wake);
        if (i !== -1) waiters.splice(i, 1);
        reject(new Error("aborted"));
      };
      const cleanup = () => {
        if (signal) signal.removeEventListener("abort", onAbort);
      };

      if (signal) signal.addEventListener("abort", onAbort, { once: true });
      waiters.push(wake);
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
        reject(new HttpError(413, "payload_too_large"));
        return;
      }

      chunks.push(chunk);
    });

    req.on("end", () => {
      const raw = Buffer.concat(chunks).toString("utf8");
      try {
        resolve(raw ? JSON.parse(raw) : {});
      } catch {
        // The caller's bug, same as a failed parseZipRequest — a raw
        // SyntaxError here would surface as a 500 and count as a build failure.
        reject(new HttpError(400, "invalid_json"));
      }
    });

    req.on("error", reject);
  });
}

function unauthorized(res: ServerResponse) {
  counters.unauthorizedRequests += 1;
  sendJson(res, 401, { error: "Unauthorized", version: VERSION });
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
  sendJson(res, 400, { success: false, error });
}

function handleHealth(res: ServerResponse) {
  const payload = healthPayload();
  // 503 on degraded storage so a dumb uptime monitor keying off the status
  // code catches a misconfigured bucket, not just a dead process.
  sendJson(res, payload.status === "ok" ? 200 : 503, payload);
}

function handleErrors(res: ServerResponse) {
  sendJson(res, 200, { counters, errors: recentErrors });
}

async function handleZip(req: IncomingMessage, res: ServerResponse) {
  const requestId = req.headers["x-request-id"]?.toString() || randomUUID();
  if (!req.headers["content-type"]?.includes("application/json"))
    return badRequest(res, "content_type_must_be_json");
  if (shuttingDown)
    return sendJson(res, 503, {
      success: false,
      error: "shutting_down",
      retryable: true,
    });

  const abort = new AbortController();
  activeBuilds.add(abort);

  // 'close' fires after a normal response too, so gate on writableEnded —
  // only a socket that died before we answered means the client is gone.
  res.on("close", () => {
    if (!res.writableEnded) abort.abort();
  });

  try {
    const body = parseZipRequest(await readJsonBody(req));
    const extraEntries = body.extraEntries ?? [];

    log.info("/zip request", {
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

    counters.buildsSucceeded += 1;
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

    sendJson(res, 200, {
      success: true,
      requestId,
      finalKey: result.finalKey,
      totalFiles: result.totalFiles,
      addedFiles: result.addedFiles,
      skippedFiles: result.skippedFiles,
      // The caller records the licence in its manifest only when this
      // confirms it shipped — absent means a worker that predates extras.
      addedExtraEntries: result.addedExtraEntries,
    });
  } catch (err: any) {
    // An abort is either the client hanging up (socket gone — write nothing)
    // or a shutdown drain (socket alive — tell the caller to retry). Neither
    // is a build failure; don't pollute the failure counters.
    if (abort.signal.aborted) {
      counters.abortedBuilds += 1;
      log.info("/zip aborted", { requestId, shuttingDown });
      if (!res.destroyed && !res.writableEnded) {
        sendJson(res, 503, {
          success: false,
          error: "shutting_down",
          retryable: true,
        });
      }
      return;
    }

    // Queue-full is shed load, not a failure: the caller retries.
    if (err instanceof QueueFullError) {
      counters.queueRejected += 1;
      return sendJson(res, err.status, {
        success: false,
        error: err.message,
        retryable: true,
      });
    }

    // Validation failures are the caller's bug, not ours — surface them in
    // /errors under their own scope so FE/BE drift is visible, but keep
    // them out of the build-failure counters.
    if (err instanceof HttpError && err.status === 400) {
      counters.badRequests += 1;
      recordError("bad_request", err.message, { requestId });
      return sendJson(res, 400, { success: false, error: err.message });
    }

    const msg = err?.message ?? "Internal error";
    const status = err instanceof HttpError ? err.status : 500;
    counters.buildFailures += 1;
    counters.lastBuildAt = new Date().toISOString();
    counters.lastBuildOk = false;
    recordError("build_failed", msg, { requestId });
    sendJson(res, status, { success: false, error: msg });
  } finally {
    activeBuilds.delete(abort);
  }
}

async function handleRequest(req: IncomingMessage, res: ServerResponse) {
  // /health stays ahead of the API-key check on purpose: uptime monitors
  // don't carry credentials, and a locked-out health check reads as an
  // outage instead of a config problem.
  if (req.method === "GET" && req.url === "/health") return handleHealth(res);

  if (!checkApiKey(req)) return unauthorized(res);

  if (req.method === "GET" && req.url === "/errors") return handleErrors(res);
  if (req.method === "POST" && req.url === "/zip") return handleZip(req, res);

  sendJson(res, 404, { error: "Not found" });
}

const server = createServer(handleRequest);
server.keepAliveTimeout = 70_000;
server.headersTimeout = 75_000;
server.requestTimeout = 0; // let your own job timeout logic control this

// ---------------------------------------------------------------------------
// Graceful shutdown: without this, node's default SIGTERM action kills the
// process instantly — every deploy during a build resets the caller's
// download and strands the half-uploaded multipart parts in the bucket.
// Compose must allow at least SHUTDOWN_GRACE_MS before SIGKILL
// (stop_grace_period), or none of this gets to run.
// ---------------------------------------------------------------------------
const SHUTDOWN_GRACE_MS = envInt("SHUTDOWN_GRACE_MS", 5 * 60_000);
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

async function shutdown(sig: string) {
  if (shuttingDown) return;
  shuttingDown = true;
  log.info(`${sig} received, draining`, {
    inFlight,
    queued: waiters.length,
    graceMs: SHUTDOWN_GRACE_MS,
  });
  server.close();

  const deadline = Date.now() + SHUTDOWN_GRACE_MS;
  while ((inFlight > 0 || waiters.length > 0) && Date.now() < deadline) {
    await sleep(1_000);
  }

  if (inFlight > 0 || waiters.length > 0) {
    log.warn("grace expired, aborting remaining builds", {
      inFlight,
      queued: waiters.length,
    });
    for (const ac of activeBuilds) ac.abort();
    // Give the abort path a moment to abort the multipart uploads cleanly.
    const cleanupDeadline = Date.now() + 10_000;
    while (inFlight > 0 && Date.now() < cleanupDeadline) await sleep(250);
  }

  log.info("drained, exiting");
  process.exit(0);
}

function main() {
  server.listen(PORT, () => {
    log.info(`listening on http://localhost:${PORT}`);
    log.info(`version: ${VERSION}`);
    log.info("limits", {
      maxConcurrentZips: MAX_CONCURRENT_ZIPS,
      maxQueueDepth: MAX_QUEUE_DEPTH,
    });
    log.info("storage", {
      endpoint: zipBuilder.endpointHost,
      bucket: zipBuilder.bucketName,
    });
    void runStorageProbe();
    setInterval(() => void runStorageProbe(), PROBE_INTERVAL_MS).unref();
  });

  process.on("SIGTERM", () => void shutdown("SIGTERM"));
  process.on("SIGINT", () => void shutdown("SIGINT"));
}

// Importable without side effects (tests require() this module); only a
// direct `node dist/server.js` / ts-node invocation boots the server.
if (require.main === module) main();

export {
  parseZipRequest,
  sanitizeExtraEntries,
  hasDotDotSegment,
  withZipSlot,
  readJsonBody,
  HttpError,
  QueueFullError,
  MAX_CONCURRENT_ZIPS,
  MAX_QUEUE_DEPTH,
};
