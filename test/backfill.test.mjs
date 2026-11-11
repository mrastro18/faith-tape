// Backfill tests against a fake candles endpoint. No network.
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, existsSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { backfill } from "../recorder/backfill.mjs";

// A fake that serves one candle every five minutes in the asked range, newest first, as Coinbase does.
function fake(fail) {
  const calls = [];
  const fetchImpl = async (url) => {
    const u = new URL(url);
    calls.push(u.searchParams.get("start"));
    if (fail && fail(u)) return { status: 500, text: async () => "no", headers: new Map() };
    const a = Date.parse(u.searchParams.get("start")) / 1000;
    const b = Date.parse(u.searchParams.get("end")) / 1000;
    const out = [];
    for (let t = b; t >= a; t -= 300) out.push([t, 1, 2, 1.5, 1.6, 10]); // includes the end bound, which must be dropped
    return { status: 200, text: async () => JSON.stringify(out), headers: new Map() };
  };
  return { fetchImpl, calls };
}

const read = (p) => gunzipSync(readFileSync(p)).toString().trim().split("\n").map((l) => JSON.parse(l));

test("writes one gzip file per base and month, each five minutes once, and resumes", async () => {
  const root = mkdtempSync(join(tmpdir(), "bf-"));
  const w = fake();
  const out = await backfill({ root, bases: ["BTC"], from: "2021-02", to: "2021-04", fetchImpl: w.fetchImpl, wait: async () => {}, log: () => {} });
  assert.equal(out.written, 2);
  const feb = read(join(root, "data", "coinbase", "candles5m", "BTC", "2021-02.ndjson.gz"));
  assert.equal(feb.length, 28 * 288);
  assert.equal(feb[0][0], Date.UTC(2021, 1, 1));
  assert.equal(feb.at(-1)[0], Date.UTC(2021, 2, 1) - 300_000);
  assert.deepEqual(feb[0].slice(1), [1.5, 2, 1, 1.6, 10]); // open, high, low, close, volume
  assert.ok(feb.every((r, i) => i === 0 || r[0] - feb[i - 1][0] === 300_000));
  const again = await backfill({ root, bases: ["BTC"], from: "2021-02", to: "2021-04", fetchImpl: w.fetchImpl, wait: async () => {}, log: () => {} });
  assert.deepEqual([again.written, again.skipped], [0, 2]);
});

test("a month whose fetch fails is not written and is reported", async () => {
  const root = mkdtempSync(join(tmpdir(), "bf-"));
  const w = fake((u) => u.searchParams.get("start").startsWith("2021-03-1"));
  const out = await backfill({ root, bases: ["ETH"], from: "2021-02", to: "2021-04", fetchImpl: w.fetchImpl, wait: async () => {}, log: () => {} });
  assert.equal(out.written, 1);
  assert.equal(out.failed.length, 1);
  assert.ok(!existsSync(join(root, "data", "coinbase", "candles5m", "ETH", "2021-03.ndjson.gz")));
});
