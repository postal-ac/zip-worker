// Shared env parsing — imported by server.ts and zip-builder.ts (this file
// must not import either, to stay cycle-free).

/**
 * Numeric env var where the fallback also covers NaN: a typo like
 * MAX_BODY_BYTES=1GB must not silently disable a limit
 * (`total > NaN` is always false).
 */
export function envInt(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n)) {
    console.warn(
      `[zip-worker] ${name}=${JSON.stringify(raw)} is not a number, using ${fallback}`,
    );
    return fallback;
  }
  return n;
}
