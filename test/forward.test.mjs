// Forward-record tests against a fake candles endpoint. No network.
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, existsSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { dueDays, forward, refuseHoldout, FIRST_DAY } from "../recorder/forward.mjs";

function fake() {
  const calls = [];
  const fetchImpl = async (url) => {
    const u = new URL(url);
    calls.push(u.searchParams.get("start"));
    const a = Date.parse(u.searchParams.get("start")) / 1000;
    const b = Date.parse(u.searchParams.get("end")) / 1000;
    const out = [];
    for (let t = b; t >= a; t -= 300) out.push([t, 1, 2, 1.5, 1.6, 10]);
    return { status: 200, text: async () => JSON.stringify(out), headers: new Map() };
  };
  return { fetchImpl, calls };
}
const read = (p) => gunzipSync(readFileSync(p)).toString().trim().split("\n").map((l) => JSON.parse(l));
const quiet = { wait: async () => {}, log: () => {} };

test("never fetches a day of the 2026 holdout", () => {
  assert.equal(FIRST_DAY, "2026-11-01");
  assert.throws(() => refuseHoldout("2026-10-31"));
  assert.throws(() => refuseHoldout("2025-12-31"));
  refuseHoldout("2026-11-01");
  assert.ok(dueDays(Date.parse("2027-01-01T00:00:00Z")).every((d) => d >= FIRST_DAY));
});

test("fetches only whole days that ended at least 15 minutes ago", () => {
  assert.deepEqual(dueDays(Date.parse("2026-11-02T00:14:00Z")), []);
  assert.deepEqual(dueDays(Date.parse("2026-11-02T00:15:00Z")), ["2026-11-01"]);
  assert.deepEqual(dueDays(Date.parse("2026-11-04T12:00:00Z")), ["2026-11-01", "2026-11-02", "2026-11-03"]);
});

test("writes one file per base and day, each five minutes once, and skips what is on disk", async () => {
  const root = mkdtempSync(join(tmpdir(), "fw-"));
  const w = fake();
  const now = Date.parse("2026-11-03T06:00:00Z");
  const out = await forward({ root, bases: ["USDT", "BTC"], now, fetchImpl: w.fetchImpl, ...quiet });
  assert.deepEqual([out.written, out.skipped, out.failed.length], [4, 0, 0]);
  const d = read(join(root, "data", "forward", "coinbase", "candles5m", "USDT", "2026-11-01.ndjson.gz"));
  assert.equal(d.length, 288);
  assert.equal(d[0][0], Date.parse("2026-11-01T00:00:00Z"));
  assert.equal(d.at(-1)[0], Date.parse("2026-11-02T00:00:00Z") - 300_000);
  assert.ok(w.calls.every((s) => s >= "2026-11-01"));
  const again = await forward({ root, bases: ["USDT", "BTC"], now, fetchImpl: w.fetchImpl, ...quiet });
  assert.deepEqual([again.written, again.skipped], [0, 4]);
  assert.ok(!existsSync(join(root, "data", "forward", "coinbase", "candles5m", "USDT", "2026-11-03.ndjson.gz")));
});

test("fills a long gap over several runs", async () => {
  const root = mkdtempSync(join(tmpdir(), "fw-"));
  const now = Date.parse("2026-11-11T06:00:00Z");
  const a = await forward({ root, bases: ["USDT"], now, maxDays: 4, fetchImpl: fake().fetchImpl, ...quiet });
  assert.equal(a.written, 4);
  const b = await forward({ root, bases: ["USDT"], now, maxDays: 40, fetchImpl: fake().fetchImpl, ...quiet });
  assert.deepEqual([b.written, b.skipped], [6, 4]);
});
