// End-to-end recorder tests against fake venues. No network.
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, readdirSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { runOnce } from "../recorder/record.mjs";
import { secondBars } from "../recorder/lib.mjs";

const T0 = Date.parse("2026-11-10T12:00:00Z");

function makeWorld() {
  const w = {
    now: T0,
    bus: { THINUSDT: [], BTCUSDT: [] }, // aggTrades {a,T,p,q,m}
    cb: { "THIN-USD": [], "BTC-USD": [] }, // trades {trade_id,time,price,size,side}
    kr: { THINUSD: [], XBTUSD: [] }, // [price, vol, timeSec, side, type, misc, id]
    calls: [],
    failHosts: new Set(),
  };
  w.addBus = (sym, t, p = 1, q = 1) => w.bus[sym].push({ a: w.bus[sym].length, T: t, p: String(p), q: String(q), m: false });
  w.addCb = (prod, t, p = 1, q = 1) => w.cb[prod].push({ trade_id: w.cb[prod].length + 1, time: new Date(t).toISOString(), price: String(p), size: String(q), side: "buy" });
  w.addKr = (pair, t, p = 1, q = 1) => w.kr[pair].push([String(p), String(q), t / 1000, "s", "l", "", w.kr[pair].length + 1]);
  return w;
}

function res(status, body, headers = {}) {
  return { status, text: async () => (typeof body === "string" ? body : JSON.stringify(body)), headers: new Map(Object.entries(headers)) };
}

function fakeFetch(w) {
  return async (url) => {
    const u = new URL(url);
    w.calls.push(u.host + u.pathname);
    if (w.failHosts.has(u.host)) return res(503, "down");
    const q = Object.fromEntries(u.searchParams);
    if (u.host === "api.binance.us") {
      if (u.pathname === "/api/v3/exchangeInfo")
        return res(200, {
          symbols: Object.keys(w.bus).map((s) => ({ symbol: s, status: "TRADING", baseAsset: s.replace("USDT", ""), quoteAsset: "USDT", filters: [{ filterType: "PRICE_FILTER", tickSize: "0.01" }, { filterType: "LOT_SIZE", stepSize: "0.1" }] })),
        });
      if (u.pathname === "/api/v3/ticker/24hr") return res(200, Object.keys(w.bus).map((s) => ({ symbol: s, count: w.bus[s].length, quoteVolume: "1", bidPrice: "1", askPrice: "1.01" })));
      if (u.pathname === "/api/v3/depth") return res(200, { lastUpdateId: 7, bids: [["1", "2"]], asks: [["1.01", "3"]] });
      if (u.pathname === "/api/v3/aggTrades") {
        const all = w.bus[q.symbol].filter((t) => t.T <= w.now);
        const lim = Number(q.limit ?? 500);
        let out;
        if (q.startTime) out = all.filter((t) => t.T >= Number(q.startTime) && t.T <= Number(q.endTime)).slice(0, lim);
        else if (q.fromId != null) out = all.filter((t) => t.a >= Number(q.fromId)).slice(0, lim);
        else out = all.slice(-lim);
        return res(200, out, { "x-mbx-used-weight-1m": "10" });
      }
    }
    if (u.host === "api.exchange.coinbase.com") {
      if (u.pathname === "/products") return res(200, Object.keys(w.cb).map((id) => ({ id, base_currency: id.split("-")[0], quote_currency: "USD", status: "online", trading_disabled: false })));
      const m = u.pathname.match(/^\/products\/([^/]+)\/(trades|candles)$/);
      if (m && m[2] === "trades") {
        const all = w.cb[m[1]].filter((t) => Date.parse(t.time) <= w.now).slice().reverse(); // newest first
        const lim = Number(q.limit ?? 1000);
        const page = (q.after ? all.filter((t) => t.trade_id < Number(q.after)) : all).slice(0, lim);
        const hdr = page.length ? { "cb-after": String(page[page.length - 1].trade_id) } : {};
        return res(200, page, hdr);
      }
      if (m && m[2] === "candles") {
        const s = Date.parse(q.start);
        const e = Date.parse(q.end);
        const rows = [];
        for (let t = s; t < e; t += 60000) rows.push([t / 1000, 1, 1, 1, 1, 5]);
        return res(200, rows.reverse());
      }
    }
    if (u.host === "api.kraken.com") {
      if (u.pathname === "/0/public/AssetPairs")
        return res(200, { error: [], result: { THINUSD: { altname: "THINUSD", wsname: "THIN/USD", status: "online" }, XXBTZUSD: { altname: "XBTUSD", wsname: "XBT/USD", status: "online" } } });
      if (u.pathname === "/0/public/Trades") {
        const since = BigInt(q.since ?? "0");
        const all = w.kr[q.pair].filter((r) => r[2] * 1000 <= w.now && BigInt(Math.round(r[2] * 1e9)) > since);
        const page = all.slice(0, Number(q.count ?? 1000));
        const last = page.length ? String(BigInt(Math.round(page[page.length - 1][2] * 1e9))) : String(since);
        return res(200, { error: [], result: { [q.pair]: page, last } });
      }
    }
    return res(200, "ok");
  };
}

const config = {
  start: "2026-11-10T00:00:00Z",
  budgetSeconds: 60,
  binanceus: { quotes: ["USDT"], depthLevels: 5, firstPassPages: 1, maxPagesPerSymbol: 50, weightPause: 4500 },
  reference: { always: ["BTC", "USDT"], krakenMax: 10, coinbaseMaxPages: 50, candleChunkMinutes: 300 },
  spacingMs: {},
  probes: [{ name: "p", url: "https://example.test/ping" }],
  pages: [{ name: "fees", url: "https://example.test/fees" }],
};

function readNd(root, ...p) {
  const dir = join(root, "data", ...p);
  if (!existsSync(dir)) return [];
  const out = [];
  for (const day of readdirSync(dir).sort()) {
    const sub = join(dir, day);
    for (const f of readdirSync(sub).sort()) out.push(...readFileSync(join(sub, f), "utf8").trim().split("\n").filter(Boolean).map((l) => [f, JSON.parse(l)]));
  }
  return out;
}

async function run(w, root, cfg = config) {
  return runOnce({ root, config: cfg, fetchImpl: fakeFetch(w), now: () => w.now, wait: async (ms) => { w.now += ms; }, log: () => {} });
}

test("Binance.US tape: backfill from start, then resume by id with no duplicates and no holes", async () => {
  const w = makeWorld();
  const root = mkdtempSync(join(tmpdir(), "tape-"));
  // 2,500 trades for THIN before the run, one before the start date that must be skipped.
  w.bus.THINUSDT.push({ a: 0, T: Date.parse("2026-11-09T23:00:00Z"), p: "1", q: "1", m: false });
  for (let i = 1; i < 2500; i++) w.addBus("THINUSDT", Date.parse("2026-11-10T00:00:00Z") + i * 1000);
  const h1 = await run(w, root);
  assert.equal(h1.tasks.binanceus.ok, true);
  for (let i = 0; i < 1300; i++) w.addBus("THINUSDT", T0 + 10_000 + i * 100);
  w.now += 3_600_000;
  await run(w, root);
  const ids = readNd(root, "binanceus", "trades").filter(([f]) => f === "THINUSDT.ndjson").map(([, r]) => r[0]);
  assert.equal(ids[0], 1, "first id is the first trade at or after the start date");
  assert.equal(ids.length, 3799);
  for (let i = 1; i < ids.length; i++) assert.equal(ids[i], ids[i - 1] + 1);
});

test("an empty symbol is marked empty, not retried forever", async () => {
  const w = makeWorld();
  const root = mkdtempSync(join(tmpdir(), "tape-"));
  await run(w, root);
  const st = JSON.parse(readFileSync(join(root, "state", "state.json"), "utf8"));
  assert.equal(st.binanceus.cursors.BTCUSDT.empty, true);
});

test("Coinbase one-second bars: settled seconds only, each second written once across runs", async () => {
  const w = makeWorld();
  const root = mkdtempSync(join(tmpdir(), "tape-"));
  // Trades in the 30 s before the first run and in the run's last 3 s (unsettled).
  for (let i = 0; i < 300; i++) w.addCb("BTC-USD", T0 - 30_000 + i * 100, 100 + (i % 7), 0.5);
  for (let i = 0; i < 20; i++) w.addCb("BTC-USD", T0 - 3000 + i * 100, 200, 1);
  await run(w, root);
  w.now += 60_000;
  for (let i = 0; i < 400; i++) w.addCb("BTC-USD", T0 + 1000 + i * 100, 150, 2);
  await run(w, root);
  const bars = readNd(root, "coinbase", "bars1s").filter(([f]) => f === "BTC.ndjson").map(([, r]) => r);
  const secs = bars.map((b) => b[0]);
  assert.equal(new Set(secs).size, secs.length, "no second written twice");
  // Recompute from the raw trades for the covered seconds and compare.
  const trades = w.cb["BTC-USD"].map((t) => ({ t: Date.parse(t.time), p: Number(t.price), q: Number(t.size) }));
  const lo = secs[0];
  const hi = secs[secs.length - 1];
  const expected = secondBars(trades.filter((t) => t.t >= lo * 1000), hi + 1);
  assert.deepEqual(bars, expected);
  assert.ok(secs.includes(Math.floor((T0 - 2000) / 1000)), "a second unsettled at run 1 is written by run 2");
});

test("Coinbase candles backfill history from the start date up to where 1 s bars begin", async () => {
  const w = makeWorld();
  const root = mkdtempSync(join(tmpdir(), "tape-"));
  w.addCb("BTC-USD", T0 - 10_000, 100, 1);
  await run(w, root);
  const rows = readNd(root, "coinbase", "candles1m").filter(([f]) => f === "BTC.ndjson").map(([, r]) => r[0]);
  assert.equal(rows[0], Date.parse(config.start));
  assert.equal(new Set(rows).size, rows.length);
  const st = JSON.parse(readFileSync(join(root, "state", "state.json"), "utf8"));
  assert.ok(st.coinbase.candlesUntil.BTC <= st.coinbase.recordStart.BTC);
  assert.equal(rows[rows.length - 1], st.coinbase.candlesUntil.BTC - 60000);
});

test("Kraken bars resume with overlap but never duplicate a trade", async () => {
  const w = makeWorld();
  const root = mkdtempSync(join(tmpdir(), "tape-"));
  for (let i = 0; i < 50; i++) w.addKr("XBTUSD", T0 - 20_000 + i * 300, 50, 1);
  await run(w, root);
  w.now += 120_000;
  for (let i = 0; i < 50; i++) w.addKr("XBTUSD", T0 + i * 1000, 60, 1);
  await run(w, root);
  const bars = readNd(root, "kraken", "bars1s").filter(([f]) => f === "BTC.ndjson").map(([, r]) => r);
  const total = bars.reduce((s, b) => s + b[6], 0);
  const settled = w.kr.XBTUSD.filter((r) => r[2] * 1000 >= T0 - 60_000 && r[2] * 1000 < w.now - 5000).length;
  assert.equal(total, settled);
  assert.equal(new Set(bars.map((b) => b[0])).size, bars.length);
});

test("one venue down does not stop the others, and its status is in the health line", async () => {
  const w = makeWorld();
  const root = mkdtempSync(join(tmpdir(), "tape-"));
  w.failHosts.add("api.binance.us");
  w.addCb("BTC-USD", T0 - 10_000, 100, 1);
  const h = await run(w, root);
  assert.equal(h.tasks.binanceus.ok, false);
  assert.equal(h.tasks.binanceus.status, 503);
  assert.equal(h.tasks.coinbase.ok, true);
  assert.ok(h.http.some((x) => x.host === "api.binance.us" && x.status === 503));
});

test("daily probes run once a day and fee pages are stored only when they change", async () => {
  const w = makeWorld();
  const root = mkdtempSync(join(tmpdir(), "tape-"));
  await run(w, root);
  w.now += 3_600_000;
  const h2 = await run(w, root);
  assert.equal(h2.tasks.daily.skipped, true);
  assert.ok(existsSync(join(root, "data", "pages", "fees", "2026-11-10.html")));
});
