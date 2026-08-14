// Tests for the pure logic in src/server.ts (validation + slot queue).
// Runs against the compiled output: `npm test` builds first.
import { test } from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { PassThrough } from "node:stream";

// Must be set before the module loads: server.ts validates required env at
// boot, and these presets win over .env (dotenv never overrides existing).
process.env.STORAGE_ENDPOINT ||= "https://example.invalid";
process.env.STORAGE_REGION ||= "auto";
process.env.STORAGE_BUCKET_NAME ||= "test-bucket";
process.env.STORAGE_ACCESS_KEY_ID ||= "test-key";
process.env.STORAGE_SECRET_ACCESS_KEY ||= "test-secret";
process.env.MAX_CONCURRENT_ZIPS = "2";
process.env.MAX_QUEUE_DEPTH = "8";

const require = createRequire(import.meta.url);
const {
  parseZipRequest,
  sanitizeExtraEntries,
  hasDotDotSegment,
  withZipSlot,
  readJsonBody,
  HttpError,
  QueueFullError,
} = require("../dist/server.js");

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const validBody = () => ({
  userId: "usr_8f3k2",
  zipName: "Summer Pack.zip",
  files: [
    { filePath: "tracks/usr_8f3k2/kick.wav", fileName: "kick.wav", relPath: "Drums" },
    { filePath: "tracks/usr_8f3k2/pad.wav" },
  ],
});

const rejects400 = (body, msgPattern) =>
  assert.throws(
    () => parseZipRequest(body),
    (e) => e instanceof HttpError && e.status === 400 && msgPattern.test(e.message),
    `expected 400 matching ${msgPattern}`,
  );

test("parseZipRequest accepts a well-formed body", () => {
  const parsed = parseZipRequest(validBody());
  assert.equal(parsed.userId, "usr_8f3k2");
  assert.equal(parsed.files.length, 2);
  assert.deepEqual(parsed.extraEntries, []);
});

test("parseZipRequest names the exact broken field", () => {
  rejects400({ ...validBody(), files: [validBody().files[0], null] }, /files\[1\] is not an object/);
  rejects400(
    { ...validBody(), files: [{ filePath: "a", fileName: { nested: true } }] },
    /files\[0\]\.fileName must be a string/,
  );
  rejects400({ ...validBody(), files: [{ fileName: "x.wav" }] }, /files\[0\]\.filePath/);
});

test("parseZipRequest keeps the legacy error strings", () => {
  rejects400({ ...validBody(), files: [] }, /^no_files$/);
  rejects400(
    { ...validBody(), files: Array(5001).fill({ filePath: "a" }) },
    /^too_many_files$/,
  );
  rejects400(null, /^invalid_payload$/);
});

test("parseZipRequest rejects zip-slip attempts", () => {
  rejects400(
    { ...validBody(), files: [{ filePath: "a", relPath: "../../.." }] },
    /files\[0\]\.relPath contains/,
  );
  rejects400(
    { ...validBody(), files: [{ filePath: "a", fileName: "..\\..\\evil.exe" }] },
    /files\[0\]\.fileName contains/,
  );
  // dots inside a segment are fine — only a bare ".." segment traverses
  parseZipRequest({ ...validBody(), files: [{ filePath: "a", fileName: "v1..final.wav" }] });
});

test("parseZipRequest rejects path separators in userId", () => {
  rejects400({ ...validBody(), userId: "usr/../other" }, /userId/);
  rejects400({ ...validBody(), userId: ".." }, /userId/);
  // exotic but harmless ID formats must keep working
  parseZipRequest({ ...validBody(), userId: "auth0|12345" });
});

test("sanitizeExtraEntries drops malformed and traversing entries", () => {
  const out = sanitizeExtraEntries([
    { fileName: "LICENSE.txt", text: "ok" },
    { fileName: "../../evil", text: "slip" },
    { fileName: "no-content.txt" },
    null,
  ]);
  assert.deepEqual(out.map((e) => e.fileName), ["LICENSE.txt"]);
});

test("hasDotDotSegment matches only real traversal", () => {
  assert.equal(hasDotDotSegment("Drums/kick.wav"), false);
  assert.equal(hasDotDotSegment("v1..final.wav"), false);
  assert.equal(hasDotDotSegment("Drums/../../evil"), true);
  assert.equal(hasDotDotSegment("..\\evil"), true);
});

test("readJsonBody: malformed JSON is the caller's 400, not our 500", async () => {
  const req = new PassThrough();
  const pending = readJsonBody(req);
  req.end('{"userId": broken');
  await assert.rejects(
    pending,
    (e) => e instanceof HttpError && e.status === 400 && e.message === "invalid_json",
  );
});

test("readJsonBody: parses a valid body, empty body becomes {}", async () => {
  const req = new PassThrough();
  const pending = readJsonBody(req);
  req.end('{"userId":"u1"}');
  assert.deepEqual(await pending, { userId: "u1" });

  const empty = new PassThrough();
  const emptyPending = readJsonBody(empty);
  empty.end();
  assert.deepEqual(await emptyPending, {});
});

test("withZipSlot: aborted waiter removes itself, wake-up not wasted", async () => {
  const events = [];
  const run = (name, ms, signal) =>
    withZipSlot(async () => {
      events.push(`start:${name}`);
      await sleep(ms);
      events.push(`end:${name}`);
    }, signal).then(
      () => "done",
      (e) => e.message,
    );

  const ac = new AbortController();
  const a = run("A", 80);
  const b = run("B", 300);
  await sleep(20); // A and B claim both slots
  const c = run("C", 50, ac.signal);
  const d = run("D", 50);
  await sleep(20);

  ac.abort();
  assert.equal(await c, "aborted");

  await a; // A's completion hands its wake-up to the queue
  await sleep(20);
  // The wake-up must reach live D, not the dead C entry.
  assert.ok(events.includes("start:D"), "D started right after A finished");
  assert.ok(!events.includes("end:B"), "B was still running — D did not wait for it");
  await Promise.all([b, d]);
});

test("withZipSlot: queue cap sheds load with QueueFullError", async () => {
  const running = [withZipSlot(() => sleep(80)), withZipSlot(() => sleep(80))];
  await sleep(20);
  const queued = Array.from({ length: 8 }, () => withZipSlot(() => sleep(5)));
  await sleep(20);

  await assert.rejects(withZipSlot(() => sleep(5)), QueueFullError);

  const results = await Promise.allSettled([...running, ...queued]);
  assert.ok(results.every((r) => r.status === "fulfilled"), "capped queue drains fully");
});

test("withZipSlot: no slot leak after aborts and rejections", async () => {
  // If earlier scenarios leaked a slot, two fresh builds could not run
  // concurrently and this would take >2x as long.
  const t0 = Date.now();
  await Promise.all([withZipSlot(() => sleep(50)), withZipSlot(() => sleep(50))]);
  assert.ok(Date.now() - t0 < 200, "both slots were free");
});
