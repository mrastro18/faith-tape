// Quote poller tests against a fake bookTicker. No network.
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, readFileSync, readdirSync, writeFileSync, existsSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pollQuotes, quoteSymbols } from "../recorder/quotes.mjs";

const STATE = {
  binanceus: {
    pairs: [
      { symbol: "THINUSD", base: "THIN", quote: "USD" },
      { symbol: "THINUSDT", base: "THIN", quote: "USDT" },
      { symbol: "BTCUSDT", base: "BTC", quote: "USDT" },
    ],
    universe: { THINUSD: { in: true }, THINUSDT: { in: false }, BTCUSDT: { in: false } },
  },
};

function setup(state = STATE) {
  const root = mkdtempSync(join(tmpdir(), "quotes-"));
  mkdirSync(join(root, "state"), { recursive: true });
  writeFileSync(join(root, "state", "state.json"), JSON.stringify(state));
  return root;
}

// A clock that only moves when the poller waits, plus 50 ms per answer.
function world(start, book) {
  const w = { t: start, calls: 0, status: null };
  w.now = () => w.t;
  w.wait = async (ms) => {
    w.t += ms;
  };
  w.fetchImpl = async () => {
    w.calls += 1;
    w.t += 50;
    if (w.status) return { status: w.status, text: async () => "no", headers: new Map() };
    const list = book(w.t, w.calls).map(([symbol, b, bq, a, aq]) => ({
      symbol,
      bidPrice: String(b),
      bidQty: String(bq),
      askPrice: String(a),
      askQty: String(aq),
    }));
    return { status: 200, text: async () => JSON.stringify(list), headers: new Map() };
  };
  return w;
}

// Every session file of a day, in order; each must be sealed (gzip, no plain file left behind).
const rowsOf = (root, day) => {
  const dir = join(root, "data", "binanceus", "quotes", day);
  if (!existsSync(dir)) return [];
  const names = readdirSync(dir).sort();
  assert.ok(names.every((n) => n.endsWith(".ndjson.gz")), `unsealed: ${names}`);
  return names.flatMap((n) => gunzipSync(readFileSync(join(dir, n))).toString("utf8").trim().split("\n").map((l) => JSON.parse(l)));
};

const config = { spacingMs: {}, quotes: { everyMs: 5000, snapshotEveryMs: 60_000 } };

test("records every leg of a universe base, and nothing else", () => {
  assert.deepEqual(quoteSymbols(STATE), ["THINUSD", "THINUSDT"]);
});

test("writes a snapshot, then only price changes, then an end row", async () => {
  const root = setup();
  const T0 = Date.parse("2026-11-10T12:00:00Z");
  // THINUSD's bid steps up on the 3rd answer; its size changes on the 5th (no row); BTCUSDT is ignored.
  const w = world(T0, (t, n) => [
    ["THINUSD", n >= 3 ? 1.01 : 1.0, n >= 5 ? 9 : 5, 1.02, 5],
    ["THINUSDT", 0.99, 1, 1.03, 1],
    ["BTCUSDT", 100 + n, 1, 101 + n, 1],
  ]);
  const out = await pollQuotes({ root, config, untilMs: T0 + 30_000, fetchImpl: w.fetchImpl, now: w.now, wait: w.wait, log: () => {} });
  const rows = rowsOf(root, "2026-11-10");
  assert.equal(rows[0][0], "s");
  assert.deepEqual(
    rows.filter((r) => r[0] === "f").map((r) => r[2]),
    ["THINUSD", "THINUSDT"],
  );
  const q = rows.filter((r) => r[0] === "q");
  assert.equal(q.length, 1);
  assert.deepEqual(q[0].slice(2), ["THINUSD", 1.01, 5, 1.02, 5]);
  const e = rows.at(-1);
  assert.equal(e[0], "e");
  assert.equal(e[2], out.polls);
  assert.ok(out.polls >= 5 && out.polls <= 6, `polls ${out.polls}`);
  assert.ok(rows.every((r) => !r.includes("BTCUSDT")));
  assert.ok(w.t <= T0 + 30_000 + 50);
});

test("repeats the full snapshot on schedule", async () => {
  const root = setup();
  const T0 = Date.parse("2026-11-10T12:00:00Z");
  const w = world(T0, () => [["THINUSD", 1, 1, 2, 1]]);
  await pollQuotes({ root, config, untilMs: T0 + 130_000, fetchImpl: w.fetchImpl, now: w.now, wait: w.wait, log: () => {} });
  const f = rowsOf(root, "2026-11-10").filter((r) => r[0] === "f");
  assert.equal(f.length, 3); // at 0, ~60 s and ~120 s
});

test("a session that crosses midnight ends and restarts in the new day's file", async () => {
  const root = setup();
  const T0 = Date.parse("2026-11-10T23:59:50Z");
  const w = world(T0, (t) => [["THINUSD", t < Date.parse("2026-11-11T00:00:00Z") ? 1 : 1.5, 1, 2, 1]]);
  await pollQuotes({ root, config, untilMs: T0 + 30_000, fetchImpl: w.fetchImpl, now: w.now, wait: w.wait, log: () => {} });
  const a = rowsOf(root, "2026-11-10");
  const b = rowsOf(root, "2026-11-11");
  assert.equal(readdirSync(join(root, "data", "binanceus", "quotes", "2026-11-11")).length, 1);
  assert.match(readdirSync(join(root, "data", "binanceus", "quotes", "2026-11-11"))[0], /^000\d{3}Z-[^./]+\.ndjson\.gz$/);
  assert.deepEqual([a[0][0], a.at(-1)[0]], ["s", "e"]);
  assert.deepEqual([b[0][0], b[1][0], b.at(-1)[0]], ["s", "f", "e"]);
  assert.equal(b[1][3], 1.5);
});

test("stops at a rate limit without retrying, and still closes the session", async () => {
  const root = setup();
  const T0 = Date.parse("2026-11-10T12:00:00Z");
  const w = world(T0, () => [["THINUSD", 1, 1, 2, 1]]);
  const wait = async (ms) => {
    await w.wait(ms);
    if (w.calls >= 2) w.status = 429;
  };
  const out = await pollQuotes({ root, config, untilMs: T0 + 60_000, fetchImpl: w.fetchImpl, now: w.now, wait, log: () => {} });
  assert.equal(out.stopped, "HTTP 429");
  assert.equal(w.calls, 3);
  const rows = rowsOf(root, "2026-11-10");
  assert.equal(rows.at(-1)[0], "e");
  assert.equal(rows.at(-1)[3], 1);
  assert.deepEqual(rows.at(-2).filter((x, i) => i !== 1), ["x", 429]);
});

test("over the session byte budget only snapshots are written, after a marker", async () => {
  const root = setup();
  const T0 = Date.parse("2026-11-10T12:00:00Z");
  const w = world(T0, (t, n) => [["THINUSD", 1 + n / 100, 1, 2, 1]]);
  const cfg = { ...config, quotes: { ...config.quotes, maxBytesPerSession: 10 } };
  const out = await pollQuotes({ root, config: cfg, untilMs: T0 + 30_000, fetchImpl: w.fetchImpl, now: w.now, wait: w.wait, log: () => {} });
  const rows = rowsOf(root, "2026-11-10");
  assert.equal(rows.filter((r) => r[0] === "q").length, 0);
  assert.equal(rows.filter((r) => r[0] === "f").length, 1);
  assert.equal(rows.filter((r) => r[0] === "c").length, 1);
  assert.equal(rows.at(-1)[4], true);
  assert.equal(out.capped, true);
});

test("does nothing without a universe", async () => {
  const root = setup({});
  const out = await pollQuotes({ root, config, untilMs: Date.now() + 1e6, fetchImpl: async () => assert.fail("no call") });
  assert.equal(out.stopped, "no symbols");
});

test("the bridge stream writes the same rows under quotes-bridge and leaves quotes alone", async () => {
  const root = setup();
  const T0 = Date.parse("2026-11-10T12:00:00Z");
  const w = world(T0, () => [["THINUSD", 1, 1, 2, 1]]);
  await pollQuotes({ root, config, untilMs: T0 + 20_000, stream: "quotes-bridge", fetchImpl: w.fetchImpl, now: w.now, wait: w.wait, log: () => {} });
  assert.equal(existsSync(join(root, "data", "binanceus", "quotes")), false);
  const dir = join(root, "data", "binanceus", "quotes-bridge", "2026-11-10");
  const names = readdirSync(dir);
  assert.equal(names.length, 1);
  const rows = gunzipSync(readFileSync(join(dir, names[0]))).toString("utf8").trim().split("\n").map((l) => JSON.parse(l));
  assert.equal(rows[0][0], "s");
  assert.equal(rows.at(-1)[0], "e");
  await assert.rejects(pollQuotes({ root, config, untilMs: T0, stream: "../x", fetchImpl: w.fetchImpl, now: w.now, wait: w.wait, log: () => {} }), /unknown stream/);
});
