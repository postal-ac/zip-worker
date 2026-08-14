// Tests for the builder-side zip-slip sanitization (second defense layer).
// buildEntryPath is private in TS; that is compile-time only, so the
// compiled output exposes it for testing without widening the API.
import { test } from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const { ZipBuilder } = require("../dist/storage/zip-builder.js");

const builder = new ZipBuilder({
  endpoint: "https://example.invalid",
  region: "auto",
  bucket: "test-bucket",
  accessKeyId: "test-key",
  secretAccessKey: "test-secret",
});

const entry = (rel, name) => builder.buildEntryPath(rel, name);

test("clean inputs keep their existing entry paths", () => {
  assert.equal(entry("Drums", "kick.wav"), "Drums/kick.wav");
  assert.equal(entry(undefined, "kick.wav"), "kick.wav");
  assert.equal(entry(undefined, "/kick.wav"), "kick.wav"); // legacy leading-slash strip
  assert.equal(entry("Drums/", "kick.wav"), "Drums/kick.wav");
  assert.equal(entry("Stems", "Drums/kick.wav"), "Stems/Drums/kick.wav");
  assert.equal(entry("Drums//loops", "kick.wav"), "Drums/loops/kick.wav");
});

test("traversal segments are neutralized (zip-slip)", () => {
  assert.equal(entry("../../..", "malware.exe"), "malware.exe");
  assert.equal(entry("Drums", "../../evil.exe"), "Drums/evil.exe");
  assert.equal(entry("Drums/../../x", "kick.wav"), "Drums/x/kick.wav");
  assert.equal(entry(undefined, "..\\..\\evil.exe"), "evil.exe");
  assert.equal(entry("./Drums/.", "kick.wav"), "Drums/kick.wav");
  assert.equal(entry("..", ".."), "file"); // all-garbage falls back, never empty
});
