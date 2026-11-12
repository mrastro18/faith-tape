// Coinbase and Kraken quote tapes against fake answers in the documented shapes. No network.
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, readFileSync, readdirSync, writeFileSync, existsSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pollQuotes } from "../recorder/quotes.mjs";
import { parseBook1 } from "../recorder/venues/coinbase.mjs";
import { parseTicker } from "../recorder/venues/kraken.mjs";

// Documented examples, copied: Coinbase Exchange "Get product book" and Kraken "Get Ticker Information".
const CB_BOOK_DOC = { sequence: 13051505638, bids: [["6247.58", "6.3578146", 2]], asks: [["6251.52", "2", 1]], time: "2021-02-12T01:09:23.334Z" };
const KR_TICKER_DOC = {
  error: [],
  result: {
    XXBTZUSD: {
      a: ["30300.10000", "1", "1.000"],
      b: ["30300.00000", "1", "1.000"],
      c: ["30303.20000", "0.00067643"],
      v: ["4083.67001100", "4412.73601799"],
      p: ["30706.77771", "30689.13205"],
      t: [34619, 38907],
      l: ["29868.30000", "29868.30000"],
      h: ["31631.00000", "31631.00000"],
      o: "30502.80000",
    },
  },
};

const CB = ["BTC-USD", "ETH-USD"];
const KR = { XBTUSD: "XXBTZUSD", XDGUSD: "XDGUSD" };
const cfgFor = (venues, extra = {}) => ({ spacingMs: {}, quotes: { everyMs: 5000, snapshotEveryMs: 60_000, ...extra, venues } });
const VENUES = {
  coinbase: cfgFor({ coinbase: { products: CB, spacingMs: 200 } }),
  kraken: cfgFor({ kraken: { pairs: KR } }),
};

function setup(state = {}) {
  const root = mkdtempSync(join(tmpdir(), "quotes-v-"));
  mkdirSync(join(root, "state"), { recursive: true });
  writeFileSync(join(root, "state", "state.json"), JSON.stringify(state));
  return root;
}

// A book per symbol: book(t, n, symbol) -> [bid, bidQty, ask, askQty] or a number (an HTTP status) for a failure.
// Answers are built in each venue's documented shape. The clock moves only when the poller waits, plus 50 ms per call.
function world(start, book, clock = { t: start }) {
  const w = { calls: [] };
  Object.defineProperty(w, "t", { get: () => clock.t, set: (v) => (clock.t = v) });
  w.now = () => w.t;
  w.wait = async (ms) => {
    w.t += ms;
  };
  const reply = (status, body) => ({ status, text: async () => (typeof body === "string" ? body : JSON.stringify(body)), headers: new Map() });
  w.fetchImpl = async (url) => {
    w.calls.push(url);
    w.t += 50;
    const n = w.calls.length;
    const u = new URL(url);
    if (u.host === "api.exchange.coinbase.com") {
      const m = u.pathname.match(/^\/products\/([^/]+)\/book$/);
      assert.ok(m && u.searchParams.get("level") === "1", `unexpected ${url}`);
      const q = book(w.t, n, decodeURIComponent(m[1]));
      if (typeof q === "number") return reply(q, "no");
      const [b, bq, a, aq] = q;
      return reply(200, { sequence: n, bids: [[String(b), String(bq), 1]], asks: [[String(a), String(aq), 3]], time: new Date(w.t).toISOString() });
    }
    if (u.host === "api.kraken.com") {
      assert.equal(u.pathname, "/0/public/Ticker");
      const asked = u.searchParams.get("pair").split(",");
      const result = {};
      for (const s of asked) {
        const q = book(w.t, n, s);
        if (typeof q === "number") return reply(q, "no");
        if (typeof q === "string") return reply(200, { error: [q] });
        if (q === null) continue; // left out of the answer
        const [b, bq, a, aq] = q;
        result[KR[s] ?? s] = { a: [String(a), "1", String(aq)], b: [String(b), "1", String(bq)], c: [String(b), "0.1"], v: ["1", "2"], p: ["1", "1"], t: [1, 2], l: ["1", "1"], h: ["1", "1"], o: "1" };
      }
      return reply(200, { error: [], result });
    }
    if (u.host === "api.binance.us") {
      const list = [["THINUSD", 1 + (n % 3) / 100, 5, 1.05, 5]].map(([symbol, b, bq, a, aq]) => ({ symbol, bidPrice: String(b), bidQty: String(bq), askPrice: String(a), askQty: String(aq) }));
      return reply(200, list);
    }
    throw new Error(`unexpected host ${u.host}`);
  };
  return w;
}

const filesOf = (root, venue, day) => {
  const dir = join(root, "data", venue, "quotes", day);
  return existsSync(dir) ? readdirSync(dir).sort() : [];
};
// Every session file of a day, in order; each must be sealed (gzip, no plain file left behind).
const rowsOf = (root, venue, day) => {
  const names = filesOf(root, venue, day);
  assert.ok(names.every((n) => /^\d{6}Z-[^./]+\.ndjson\.gz$/.test(n)), `unsealed or misnamed: ${names}`);
  const dir = join(root, "data", venue, "quotes", day);
  return names.flatMap((n) => gunzipSync(readFileSync(join(dir, n))).toString("utf8").trim().split("\n").map((l) => JSON.parse(l)));
};
const run = (root, config, w, untilMs) => pollQuotes({ root, config, untilMs, fetchImpl: w.fetchImpl, now: w.now, wait: w.wait, log: () => {} });
const sym = { coinbase: CB, kraken: Object.keys(KR) };

test("coinbase: parses the documented level-1 book", () => {
  assert.deepEqual(parseBook1(CB_BOOK_DOC), [6247.58, 6.3578146, 6251.52, 2]);
  assert.deepEqual(parseBook1({ ...CB_BOOK_DOC, asks: [] }), [6247.58, 6.3578146, null, null]);
  assert.throws(() => parseBook1({ message: "NotFound" }));
});

test("kraken: parses the documented ticker by its answer key or by the name asked for", () => {
  assert.deepEqual(parseTicker(KR_TICKER_DOC, [["XBTUSD", "XXBTZUSD"]]), { quotes: [["XBTUSD", 30300, 1, 30300.1, 1]], missing: [] });
  const byAlt = { error: [], result: { XBTUSD: KR_TICKER_DOC.result.XXBTZUSD } };
  assert.deepEqual(parseTicker(byAlt, [["XBTUSD", "XXBTZUSD"], ["XDGUSD", "XDGUSD"]]).missing, ["XDGUSD"]);
  assert.throws(() => parseTicker({ error: ["EService:Unavailable"] }, []), (e) => e.xStatus === "EService:Unavailable" && !e.rateLimited);
  assert.throws(() => parseTicker({ error: ["EAPI:Rate limit exceeded"] }, []), (e) => e.rateLimited === true);
});

for (const venue of ["coinbase", "kraken"]) {
  const [A, B] = sym[venue];

  test(`${venue}: a snapshot, then only price changes, then an end row, sealed to gzip in its own directory`, async () => {
    const root = setup();
    const T0 = Date.parse("2026-11-11T12:00:00Z");
    // A's bid steps up from the 3rd poll on; B's size changes on the 4th (no row).
    let polls = 0;
    let seen = new Set();
    const w = world(T0, (t, n, s) => {
      if (seen.has(s)) {
        polls += 1;
        seen = new Set();
      }
      seen.add(s);
      const p = polls + 1;
      return s === A ? [p >= 3 ? 101 : 100, 1, 102, 1] : [10, p >= 4 ? 9 : 5, 11, 5];
    });
    const out = await run(root, VENUES[venue], w, T0 + 30_000);
    const rows = rowsOf(root, venue, "2026-11-11");
    assert.equal(filesOf(root, venue, "2026-11-11").length, 1);
    assert.equal(filesOf(root, "binanceus", "2026-11-11").length, 0);
    assert.equal(rows[0][0], "s");
    assert.equal(rows[0][2], process.env.GITHUB_RUN_ID ?? "local");
    assert.deepEqual(rows.filter((r) => r[0] === "f").map((r) => r[2]), [A, B]);
    assert.deepEqual(rows[1].slice(2), [A, 100, 1, 102, 1]);
    const q = rows.filter((r) => r[0] === "q");
    assert.equal(q.length, 1);
    assert.deepEqual(q[0].slice(2), [A, 101, 1, 102, 1]);
    assert.ok(rows.indexOf(q[0]) > rows.findLastIndex((r) => r[0] === "f"));
    const e = rows.at(-1);
    assert.equal(e[0], "e");
    const vo = out.venues[venue];
    assert.equal(e[2], vo.polls);
    assert.ok(vo.polls >= 5 && vo.polls <= 6, `polls ${vo.polls}`);
    assert.equal(out.stopped, "no symbols"); // Binance.US had no universe here; the venue ran regardless
    if (venue === "kraken") assert.equal(w.calls.length, vo.polls); // one call per poll for every pair
    else assert.equal(w.calls.length, vo.polls * CB.length);
  });

  test(`${venue}: a failed poll is an "x" row and polling goes on`, async () => {
    const root = setup();
    const T0 = Date.parse("2026-11-11T12:00:00Z");
    let n0 = null;
    const w = world(T0, (t, n, s) => {
      n0 ??= n;
      // the second poll fails as a whole (every product of it)
      return t >= T0 + 5_000 && t < T0 + 9_000 ? 503 : [s === A ? 100 + Math.floor((t - T0) / 5000) : 10, 1, 200, 1];
    });
    const out = await run(root, VENUES[venue], w, T0 + 25_000);
    const rows = rowsOf(root, venue, "2026-11-11");
    const x = rows.filter((r) => r[0] === "x");
    assert.equal(x.length, 1);
    assert.deepEqual([x[0][0], x[0][2], x[0].length], ["x", 503, 3]);
    assert.ok(rows.filter((r) => r[0] === "q" && r[1] > x[0][1]).length >= 2, "polling continued after the failure");
    assert.equal(rows.at(-1)[3], 1); // the end row counts one failed poll
    assert.equal(out.venues[venue].stopped, null);
  });

  test(`${venue}: a session that crosses midnight ends and restarts in the new day's file`, async () => {
    const root = setup();
    const T0 = Date.parse("2026-11-10T23:59:50Z");
    const mid = Date.parse("2026-11-11T00:00:00Z");
    const w = world(T0, (t, n, s) => [t < mid ? 1 : 1.5, 1, 2, 1]);
    await run(root, VENUES[venue], w, T0 + 30_000);
    const a = rowsOf(root, venue, "2026-11-10");
    const b = rowsOf(root, venue, "2026-11-11");
    assert.equal(filesOf(root, venue, "2026-11-11").length, 1);
    assert.match(filesOf(root, venue, "2026-11-11")[0], /^000\d{3}Z-[^./]+\.ndjson\.gz$/);
    assert.deepEqual([a[0][0], a.at(-1)[0]], ["s", "e"]);
    assert.ok(a.every((r) => r[1] < mid));
    assert.deepEqual([b[0][0], b[1][0], b.at(-1)[0]], ["s", "f", "e"]);
    assert.ok(b.every((r) => r[1] >= mid));
    assert.equal(b[1][3], 1.5);
  });
}

test("coinbase: one product failing alone is an x row naming it; the others are still written", async () => {
  const root = setup();
  const T0 = Date.parse("2026-11-11T12:00:00Z");
  const w = world(T0, (t, n, s) => (s === "ETH-USD" && t > T0 + 4_000 ? 500 : [s === "BTC-USD" ? 100 + Math.floor((t - T0) / 5000) : 10, 1, 200, 1]));
  const out = await run(root, VENUES.coinbase, w, T0 + 20_000);
  const rows = rowsOf(root, "coinbase", "2026-11-11");
  const x = rows.filter((r) => r[0] === "x");
  assert.ok(x.length >= 2);
  assert.ok(x.every((r) => r[2] === 500 && r[3] === "ETH-USD"));
  assert.ok(rows.filter((r) => r[0] === "q" && r[2] === "BTC-USD").length >= 2);
  assert.equal(rows.at(-1)[3], 0); // not a failed poll
  assert.equal(out.venues.coinbase.misses, x.length);
});

test("coinbase: a rate limit stops the venue at once without retrying, and closes its session", async () => {
  const root = setup();
  const T0 = Date.parse("2026-11-11T12:00:00Z");
  const w = world(T0, (t, n) => (n > CB.length ? 429 : [1, 1, 2, 1]));
  const out = await run(root, VENUES.coinbase, w, T0 + 60_000);
  assert.equal(out.venues.coinbase.stopped, "HTTP 429");
  assert.equal(w.calls.length, CB.length + 1);
  const rows = rowsOf(root, "coinbase", "2026-11-11");
  assert.deepEqual([rows.at(-2)[0], rows.at(-2)[2], rows.at(-1)[0]], ["x", 429, "e"]);
});

test("kraken: an error answer is an x row with its text; a rate-limit answer stops the venue", async () => {
  const root = setup();
  const T0 = Date.parse("2026-11-11T12:00:00Z");
  const w = world(T0, (t, n) => (n === 2 ? "EService:Unavailable" : n >= 4 ? "EAPI:Rate limit exceeded" : [1 + n / 10, 1, 2, 1]));
  const out = await run(root, VENUES.kraken, w, T0 + 60_000);
  const x = rowsOf(root, "kraken", "2026-11-11").filter((r) => r[0] === "x").map((r) => r[2]);
  assert.deepEqual(x, ["EService:Unavailable", "EAPI:Rate limit exceeded"]);
  assert.equal(out.venues.kraken.stopped, "EAPI:Rate limit exceeded");
  assert.equal(w.calls.length, 4);
});

test("kraken: a pair left out of the answer is an x row naming it; the key comes from state when the recorder knows it", async () => {
  const root = setup({ kraken: { pairs: { BTC: { key: "XXBTZUSD", altname: "XBTUSD" } } } });
  const T0 = Date.parse("2026-11-11T12:00:00Z");
  const w = world(T0, (t, n, s) => (s === "XDGUSD" ? null : [1, 1, 2, 1]));
  await run(root, cfgFor({ kraken: { pairs: { XBTUSD: null, XDGUSD: "XDGUSD" } } }), w, T0 + 12_000);
  const rows = rowsOf(root, "kraken", "2026-11-11");
  assert.deepEqual(rows.filter((r) => r[0] === "f").map((r) => r[2]), ["XBTUSD"]);
  assert.ok(rows.filter((r) => r[0] === "x").every((r) => r[2] === "missing" && r[3] === "XDGUSD"));
  assert.ok(rows.some((r) => r[0] === "x"));
});

test("each venue has its own byte budget", async () => {
  const root = setup();
  const T0 = Date.parse("2026-11-11T12:00:00Z");
  const w = world(T0, (t, n) => [1 + n / 100, 1, 2, 1]);
  const out = await run(root, cfgFor({ kraken: { pairs: KR, maxBytesPerSession: 10 } }), w, T0 + 30_000);
  const rows = rowsOf(root, "kraken", "2026-11-11");
  assert.equal(rows.filter((r) => r[0] === "q").length, 0);
  assert.equal(rows.filter((r) => r[0] === "c").length, 1);
  assert.equal(out.venues.kraken.capped, true);
});

// A clock for loops that run side by side: each wait is a timer, and the earliest timer fires once every loop waits.
function sharedClock(start) {
  const c = { t: start, timers: [], seq: 0 };
  c.now = () => c.t;
  c.wait = (ms) =>
    new Promise((res) => {
      c.timers.push({ at: c.t + Math.max(0, ms), seq: c.seq++, res });
      setImmediate(() => {
        c.timers.sort((a, b) => a.at - b.at || a.seq - b.seq);
        const x = c.timers.shift();
        if (!x) return;
        c.t = Math.max(c.t, x.at);
        x.res();
      });
    });
  return c;
}

test("a venue that keeps failing stops alone: Binance.US and the other venue go on", async () => {
  const root = setup({
    binanceus: { pairs: [{ symbol: "THINUSD", base: "THIN", quote: "USD" }], universe: { THINUSD: { in: true } } },
  });
  const T0 = Date.parse("2026-11-11T12:00:00Z");
  const c = sharedClock(T0);
  const w = world(T0, (t, n, s) => (KR[s] ? 503 : [100 + (n % 4), 1, 200, 1]), c);
  const cfg = cfgFor({ coinbase: { products: CB, spacingMs: 200 }, kraken: { pairs: KR } });
  const out = await pollQuotes({ root, config: cfg, untilMs: T0 + 60_000, fetchImpl: w.fetchImpl, now: c.now, wait: c.wait, log: () => {} });
  assert.equal(out.venues.kraken.stopped, "HTTP 503");
  assert.equal(out.venues.kraken.failures, 6);
  assert.equal(out.venues.coinbase.stopped, null);
  assert.ok(out.venues.coinbase.polls >= 10, `coinbase polls ${out.venues.coinbase.polls}`);
  assert.equal(out.stopped, null);
  assert.ok(out.polls >= 10, `binance polls ${out.polls}`);
  assert.equal(rowsOf(root, "binanceus", "2026-11-11").at(-1)[0], "e");
  assert.equal(rowsOf(root, "coinbase", "2026-11-11").at(-1)[0], "e");
  assert.deepEqual(filesOf(root, "kraken", "2026-11-11"), []); // never had a good answer, so no session file
});

test("coinbase and kraken: a size change alone goes to the venue's quotesz file, a miss is mirrored there", async () => {
  for (const venue of ["coinbase", "kraken"]) {
    const root = setup();
    const T0 = Date.parse("2026-11-11T12:00:00Z");
    const [A, B] = sym[venue];
    const w = world(T0, (t, n, s) => {
      const k = Math.floor((t - T0) / 5000); // the poll's index
      if (s === B && k === 2) return venue === "kraken" ? null : 500; // B alone misses one poll
      return s === A ? [100, k >= 3 ? 4 : 1, 102, 1] : [10, 1, 11, 1];
    });
    await run(root, VENUES[venue], w, T0 + 30_000);
    const dir = join(root, "data", venue, "quotesz", "2026-11-11");
    assert.deepEqual(readdirSync(dir), filesOf(root, venue, "2026-11-11"));
    const z = readdirSync(dir).flatMap((n) => gunzipSync(readFileSync(join(dir, n))).toString("utf8").trim().split("\n").map((l) => JSON.parse(l)));
    assert.deepEqual(z.filter((r) => r[0] === "z").map((r) => r.slice(2)), [[A, 100, 4, 102, 1]], venue);
    const qx = rowsOf(root, venue, "2026-11-11").filter((r) => r[0] === "x");
    assert.equal(qx.length, 1, venue);
    assert.deepEqual(z.filter((r) => r[0] === "x"), qx, venue);
    assert.equal(rowsOf(root, venue, "2026-11-11").filter((r) => r[0] === "q").length, 0, venue);
  }
});
