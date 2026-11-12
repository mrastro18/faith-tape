// Quote coverage from file names only. No network, no quote file opened.
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { coverage, dayCoverage } from "../recorder/coverage.mjs";

test("coverage: a full day of half-hour sessions counts; a long gap does not", () => {
  const full = Array.from({ length: 48 }, (_, i) => i * 30 + 0.05);
  assert.deepEqual(dayCoverage(full), { sessions: 48, minutes: 1440, longestGapMin: 0 });
  const half = full.filter((m) => m < 600);
  const c = dayCoverage(half);
  assert.equal(c.sessions, 20);
  assert.ok(c.minutes < 720 && c.longestGapMin > 800);
});

test("coverage: manifests and checkout files are both read by name, contents never opened", () => {
  const root = mkdtempSync(join(tmpdir(), "cov-"));
  mkdirSync(join(root, "manifests"), { recursive: true });
  const files = Array.from({ length: 48 }, (_, i) => {
    const h = String(Math.floor(i / 2)).padStart(2, "0");
    const m = i % 2 ? "30" : "00";
    return { path: `data/binanceus/quotes/2026-11-20/${h}${m}03Z-9.ndjson.gz`, mode: "file", bytes: 1, sha256: "x" };
  });
  writeFileSync(join(root, "manifests", "2026-11-20.ndjson"), JSON.stringify({ asset: "a", files }) + "\n");
  // A file in the checkout that is not valid gzip: reading it would throw, so this also shows it is never opened.
  mkdirSync(join(root, "data/kraken/quotes/2026-11-19"), { recursive: true });
  writeFileSync(join(root, "data/kraken/quotes/2026-11-19/120000Z-1.ndjson.gz"), "not gzip");
  const rows = coverage(root, { now: new Date("2026-11-21T01:00:00Z") });
  const b = rows.find((r) => r.venue === "binanceus");
  const k = rows.find((r) => r.venue === "kraken");
  assert.equal(b.sessions, 48);
  assert.equal(b.counts, true);
  assert.equal(k.sessions, 1);
  assert.equal(k.counts, false);
});

test("coverage: today is partial and never counts", () => {
  const root = mkdtempSync(join(tmpdir(), "cov-"));
  mkdirSync(join(root, "data/binanceus/quotes/2026-11-21"), { recursive: true });
  writeFileSync(join(root, "data/binanceus/quotes/2026-11-21/000003Z-1.ndjson.gz"), "");
  const [r] = coverage(root, { now: new Date("2026-11-21T00:20:00Z") });
  assert.equal(r.partial, true);
  assert.equal(r.counts, false);
  assert.ok(r.minutes <= 20);
});

test("coverage: --stream reads the size stream's names alone, and quotes stays the default", () => {
  const root = mkdtempSync(join(tmpdir(), "cov-"));
  mkdirSync(join(root, "manifests"), { recursive: true });
  const f = (s, n) => ({ path: `data/binanceus/${s}/2026-11-20/${n}Z-9.ndjson.gz`, mode: "file", bytes: 1, sha256: "x" });
  writeFileSync(
    join(root, "manifests", "2026-11-20.ndjson"),
    [{ asset: "q", kind: "quotes", files: [f("quotes", "000003"), f("quotes", "003003")] }, { asset: "z", kind: "quotesz", files: [f("quotesz", "000003")] }].map((l) => JSON.stringify(l)).join("\n") + "\n",
  );
  const now = new Date("2026-11-21T01:00:00Z");
  assert.equal(coverage(root, { now })[0].sessions, 2);
  assert.equal(coverage(root, { now, stream: "quotesz" })[0].sessions, 1);
  assert.throws(() => coverage(root, { now, stream: "../x" }), /unknown stream/);
});
