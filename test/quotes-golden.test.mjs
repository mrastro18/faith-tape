// The quotes stream is unchanged by the size stream (CHANGES 2026-11-12e): fixed synthetic runs whose quotes files
// must hash to what the recorder at dedac40 (before the size stream) wrote for the same runs. queue-prov-r3 in FAITH
// reads data/binanceus/quotes from 2026-11-12 on, so its rows, their selection and the file names must not move.
// Hashes are of each file's name and its gunzipped content (the gzip layer is the same code, but zlib's bytes may
// differ between Node builds). No network.
import { test } from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pollQuotes } from "../recorder/quotes.mjs";

const KR = { XBTUSD: "XXBTZUSD", XDGUSD: "XDGUSD" };
const STATE = {
  binanceus: {
    pairs: ["AAAUSD", "AAAUSDT", "BBBUSD", "CCCUSDC"].map((symbol) => ({ symbol, base: symbol.slice(0, 3), quote: symbol.slice(3) })),
    universe: { AAAUSD: { in: true }, AAAUSDT: { in: false }, BBBUSD: { in: true }, CCCUSDC: { in: true } },
  },
};

// A deterministic book: prices step on some polls, sizes change alone on others, a whole poll fails, one Coinbase
// product fails alone, one Kraken pair is left out once. The clock moves only when the poller waits, plus 50 ms a call.
const book = (n, s) => {
  const k = s.charCodeAt(0) + s.length;
  const bid = 100 + Math.floor((n + k) / 4) / 100;
  const ask = bid + 0.05 + (n % 7 === 0 ? 0.01 : 0);
  const bq = 1 + ((n * 3 + k) % 5);
  const aq = 2 + ((n + k) % 3);
  return [bid, bq, ask, aq];
};
function world(T0) {
  const w = { t: T0, n: 0 };
  w.now = () => w.t;
  w.wait = async (ms) => {
    w.t += ms;
  };
  const reply = (status, body) => ({ status, text: async () => JSON.stringify(body), headers: new Map() });
  w.fetchImpl = async (url) => {
    w.n += 1;
    w.t += 50;
    const n = w.n;
    const u = new URL(url);
    if (n % 23 === 0) return reply(503, "no");
    if (u.host === "api.binance.us")
      return reply(
        200,
        ["AAAUSD", "AAAUSDT", "BBBUSD", "CCCUSDC", "ZZZUSD"].map((symbol) => {
          const [b, bq, a, aq] = book(n, symbol);
          return { symbol, bidPrice: String(b), bidQty: String(bq), askPrice: String(a), askQty: String(aq) };
        }),
      );
    if (u.host === "api.exchange.coinbase.com") {
      const p = decodeURIComponent(u.pathname.split("/")[2]);
      if (p === "ETH-USD" && n % 11 === 0) return reply(500, "no");
      const [b, bq, a, aq] = book(n >> 1, p);
      return reply(200, { sequence: n, bids: [[String(b), String(bq), 1]], asks: [[String(a), String(aq), 3]], time: new Date(w.t).toISOString() });
    }
    if (u.host === "api.kraken.com") {
      const result = {};
      for (const s of u.searchParams.get("pair").split(",")) {
        if (s === "XDGUSD" && n % 13 === 0) continue;
        const [b, bq, a, aq] = book(n, s);
        result[KR[s] ?? s] = { a: [String(a), "1", String(aq)], b: [String(b), "1", String(bq)], c: [String(b), "0.1"], v: ["1", "2"], p: ["1", "1"], t: [1, 2], l: ["1", "1"], h: ["1", "1"], o: "1" };
      }
      return reply(200, { error: [], result });
    }
    throw new Error(`unexpected ${url}`);
  };
  return w;
}

const RUNS = {
  // across midnight, with snapshots every minute
  binanceus: { state: STATE, quotes: { maxBytesPerSession: 3_000_000 } },
  // the cap fires part way through
  "binanceus-capped": { state: STATE, quotes: { maxBytesPerSession: 4_000 }, venue: "binanceus" },
  coinbase: { state: {}, quotes: { venues: { coinbase: { products: ["BTC-USD", "ETH-USD"], spacingMs: 200 } } } },
  kraken: { state: {}, quotes: { venues: { kraken: { pairs: KR } } } },
};

/** One fixed run; returns sha256 of every quotes file of its venue (name and gunzipped content, in name order). */
export async function goldenRun(name, { stream = "quotes", extra = {} } = {}) {
  const r = RUNS[name];
  const venue = r.venue ?? name;
  const root = mkdtempSync(join(tmpdir(), "quotes-golden-"));
  mkdirSync(join(root, "state"), { recursive: true });
  writeFileSync(join(root, "state", "state.json"), JSON.stringify(r.state));
  const T0 = Date.parse("2026-11-12T23:57:31.250Z");
  const w = world(T0);
  const config = { spacingMs: {}, quotes: { everyMs: 5000, snapshotEveryMs: 60_000, ...r.quotes, ...extra } };
  const before = process.env.GITHUB_RUN_ID;
  process.env.GITHUB_RUN_ID = "4242";
  try {
    await pollQuotes({ root, config, untilMs: T0 + 6 * 60_000, stream, fetchImpl: w.fetchImpl, now: w.now, wait: w.wait, log: () => {} });
  } finally {
    if (before === undefined) delete process.env.GITHUB_RUN_ID;
    else process.env.GITHUB_RUN_ID = before;
  }
  const h = createHash("sha256");
  const base = join(root, "data", venue, stream);
  const files = [];
  for (const d of existsSync(base) ? readdirSync(base).sort() : [])
    for (const f of readdirSync(join(base, d)).sort()) {
      files.push(`${d}/${f}`);
      h.update(`${d}/${f}\n`);
      h.update(gunzipSync(readFileSync(join(base, d, f))));
    }
  return { root, files, sha256: h.digest("hex") };
}

// Written by recorder/quotes.mjs at dedac40 (the commit before the size stream), same runs.
const GOLDEN = {
  binanceus: "03235f2a9b00ad6f422978503f09457ac77f970de2819336132e1537608b5d87",
  "binanceus-capped": "7557711219fd163f4ae9a62808f0d20a27b78f7be9197cc5f2477e9e4620f716",
  coinbase: "e5cd6dcee7d16c1fb1288fa1d7b4fdf206e491d81ad35f6b68b4e063f40ac483",
  kraken: "f625df765049a4028d820e16a37f67c5407804877b19bf8389d2ff9acc1b632f",
};

for (const name of Object.keys(RUNS))
  test(`golden: ${name} quotes files are byte for byte what the recorder wrote before the size stream`, async () => {
    const g = await goldenRun(name);
    try {
      assert.ok(g.files.length >= 1);
      assert.equal(g.sha256, GOLDEN[name], `files ${g.files.join(", ")}`);
    } finally {
      rmSync(g.root, { recursive: true, force: true });
    }
  });

test("golden: turning the size stream off changes no quotes byte, and on it writes only under quotesz", async () => {
  for (const name of Object.keys(RUNS)) {
    const off = await goldenRun(name, { extra: { sizes: false } });
    try {
      assert.equal(off.sha256, GOLDEN[name], name);
      assert.equal(existsSync(join(off.root, "data", RUNS[name].venue ?? name, "quotesz")), false);
    } finally {
      rmSync(off.root, { recursive: true, force: true });
    }
  }
  const on = await goldenRun("binanceus");
  try {
    const names = (s) => readdirSync(join(on.root, "data", "binanceus", s)).flatMap((d) => readdirSync(join(on.root, "data", "binanceus", s, d)).map((f) => `${d}/${f}`)).sort();
    assert.deepEqual(names("quotesz"), names("quotes"), "one size file per session, named as its quotes file");
    assert.deepEqual(readdirSync(join(on.root, "data", "binanceus")).sort(), ["quotes", "quotesz"]);
  } finally {
    rmSync(on.root, { recursive: true, force: true });
  }
});
