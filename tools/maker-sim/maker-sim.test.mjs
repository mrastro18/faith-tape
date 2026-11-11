// Tests for the tick-data maker simulator. Outside test/ on purpose: the record workflow runs test/*.test.mjs
// before recording, and these need nothing from the network or the environment.
//   node --test tools/maker-sim/*.test.mjs
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { gzipSync } from "node:zlib";
import { FEES, nets, summarize } from "./agg.mjs";
import { dayAllowed } from "./run.mjs";
import {
  ORDER_FIELDS,
  TICK,
  loadQuotes,
  loadTrades,
  quotesFromTicks,
  runDay,
  simulateTick,
  tradesFromTicks,
} from "./tick.mjs";

const tick = 0.01;
const base = { restMs: 60_000, latencyMs: 0, lookaheadMs: 250 };
const mk = (quotes, trades, extra = {}) => {
  const Q = quotesFromTicks(quotes);
  return { ...base, q: Q.q, s: Q.s, tr: tradesFromTicks(trades), tick, ...extra };
};
const buy = (t0, price, qty, cancels = "pessimistic") => ({ side: "buy", t0, price, qty, cancels });
// bid 100.00 x 10, ask 100.01 x 10, refreshed every 10 s to keep coverage
const flat = (until = 120_000, extra = []) => {
  const rows = [];
  for (let t = 0; t <= until; t += 10_000) rows.push({ t, bid: 100, bq: 10, ask: 100.01, aq: 10 });
  return [...rows, ...extra].sort((a, b) => a.t - b.t);
};

test("joining the bid fills once prints at the price pass the queue ahead", () => {
  const ctx = mk(flat(), [
    { t: 1000, p: 100, q: 6, side: "sell" },
    { t: 2000, p: 100, q: 4, side: "sell" },
    { t: 3000, p: 100, q: 1, side: "sell" },
  ]);
  const r = simulateTick(buy(500, 100, 1), ctx);
  assert.equal(r.status, "filled");
  assert.equal(r.fillType, "queue");
  assert.equal(r.fillT, 3000);
  assert.equal(r.ahead0, 10);
});

test("a size drop that arrives before its print is not subtracted twice (pessimistic)", () => {
  // the book shows 4 left at t=1000; the print of 6 that caused it arrives at t=1100
  const quotes = flat(120_000, [{ t: 1000, bid: 100, bq: 4, ask: 100.01, aq: 10 }]).filter(
    (r) => !(r.t === 10_000) && !(r.t === 20_000),
  );
  const ctx = mk(quotes, [
    { t: 1100, p: 100, q: 6, side: "sell" },
    { t: 2000, p: 100, q: 4, side: "sell" },
    { t: 3000, p: 100, q: 1, side: "sell" },
  ]);
  const r = simulateTick(buy(500, 100, 1), ctx);
  assert.equal(r.fillT, 3000, "the 6 and the 4 only clear the queue");
  assert.equal(r.fillType, "queue");
  // with no tolerance the same tape fills at the first print: the drop is counted twice
  const r0 = simulateTick(buy(500, 100, 1), { ...ctx, lookaheadMs: 0 });
  assert.equal(r0.fillT, 1100);
});

test("the book moving through the order with no print toward it is a cross", () => {
  const quotes = [
    { t: 0, bid: 100, bq: 10, ask: 100.01, aq: 10 },
    { t: 5000, bid: 99.98, bq: 3, ask: 99.99, aq: 2 },
    { t: 60_000, bid: 99.98, bq: 3, ask: 99.99, aq: 2 },
  ];
  const r = simulateTick(buy(100, 100, 1), mk(quotes, []));
  assert.equal(r.status, "filled");
  assert.equal(r.fillType, "cross");
  assert.equal(r.fillT, 5000);
});

test("a cross whose sweep prints arrive just after the book is labelled by the prints", () => {
  const quotes = [
    { t: 0, bid: 100, bq: 10, ask: 100.01, aq: 10 },
    { t: 5000, bid: 99.98, bq: 3, ask: 100, aq: 2 },
    { t: 60_000, bid: 99.98, bq: 3, ask: 100, aq: 2 },
  ];
  const trades = [
    { t: 5050, p: 100, q: 10, side: "sell" },
    { t: 5060, p: 99.99, q: 5, side: "sell" },
  ];
  const r = simulateTick(buy(100, 100, 1), mk(quotes, trades));
  assert.equal(r.fillType, "through");
  assert.equal(r.adj, true);
});

test("a level shown empty before its prints keeps those prints ahead of the order", () => {
  // the bid at 100 is taken by exactly the 10 ahead; the book shows it gone before the prints arrive
  const quotes = [
    { t: 0, bid: 100, bq: 10, ask: 100.01, aq: 10 },
    { t: 5000, bid: 99.99, bq: 3, ask: 100.01, aq: 10 },
    { t: 61_000, bid: 99.99, bq: 3, ask: 100.01, aq: 10 },
  ];
  const trades = [{ t: 5100, p: 100, q: 10, side: "sell" }];
  const r = simulateTick(buy(100, 100, 1), mk(quotes, trades));
  assert.equal(r.status, "expired");
  const r0 = simulateTick(buy(100, 100, 1), mk(quotes, trades, { lookaheadMs: 0 }));
  assert.equal(r0.status, "filled", "without the tolerance the prints after the empty level fill it");
});

test("optimistic cancellation moves the order up; pessimistic does not", () => {
  const quotes = flat(120_000, [{ t: 1000, bid: 100, bq: 2, ask: 100.01, aq: 10 }]).filter(
    (r) => r.t === 0 || r.t === 1000 || r.t >= 70_000,
  );
  // pessimistic caps the queue at the displayed size too: 2 ahead either way here
  const trades = [{ t: 2000, p: 100, q: 3, side: "sell" }];
  const pess = simulateTick(buy(500, 100, 1), mk(quotes, trades));
  const opt = simulateTick(buy(500, 100, 1, "optimistic"), mk(quotes, trades));
  assert.equal(pess.status, "filled");
  assert.equal(opt.status, "filled");
  // a drop at the size that leaves more than the queue: only the optimistic model moves up
  const q2 = [
    { t: 0, bid: 100, bq: 10, ask: 100.01, aq: 10 },
    { t: 400, bid: 100, bq: 20, ask: 100.01, aq: 10 }, // joiners behind
    { t: 1000, bid: 100, bq: 12, ask: 100.01, aq: 10 }, // 8 cancelled
    { t: 61_000, bid: 100, bq: 12, ask: 100.01, aq: 10 },
  ];
  const tr2 = [{ t: 2000, p: 100, q: 3, side: "sell" }];
  const o = buy(200, 100, 1);
  const p2 = simulateTick(o, mk(q2, tr2));
  const o2 = simulateTick({ ...o, cancels: "optimistic" }, mk(q2, tr2));
  assert.equal(p2.status, "expired");
  assert.equal(o2.status, "filled");
});

test("an order whose coverage ends first is unknown; improve sits with nobody ahead", () => {
  const quotes = [
    { t: 0, bid: 100, bq: 10, ask: 100.05, aq: 10 },
    { t: 10_000, bid: 100, bq: 10, ask: 100.05, aq: 10 },
    { t: 500_000, bid: 100, bq: 10, ask: 100.05, aq: 10 }, // gap over maxGapMs
  ];
  const ctx = mk(quotes, []);
  assert.equal(simulateTick(buy(1000, 100, 1), ctx).status, "unknown");
  const r = simulateTick(buy(1000, 100.01, 1), mk(quotes, [{ t: 2000, p: 100.01, q: 1, side: "sell" }]));
  assert.equal(r.status, "filled");
  assert.equal(r.ahead0, 0);
});

test("runDay writes one row per placement, side, variant and model, all the same width", () => {
  const rows = [];
  for (let t = 0; t <= 86_400_000; t += 30_000)
    rows.push({ t, bid: 100 + (t % 120_000 === 0 ? 0.01 : 0), bq: 5, ask: 100.03, aq: 5 });
  const Q = quotesFromTicks(rows);
  const trades = [];
  for (let t = 15_000; t < 86_400_000; t += 60_000) trades.push({ t, p: 100, q: 2, side: "sell" });
  const res = runDay({ q: Q.q, s: Q.s, tr: tradesFromTicks(trades), tick, dayStartMs: 0 });
  assert.equal(res.orders.length, res.placements * 8);
  assert.ok(res.orders.every((r) => r.length === ORDER_FIELDS.length));
  assert.ok(res.orders.some((r) => r[3] === 0), "something fills");
});

test("CSV loaders read Tardis columns, drop crossed rows and infer the tick", async () => {
  const dir = mkdtempSync(join(tmpdir(), "maker-sim-"));
  const q = join(dir, "q.csv.gz");
  const t = join(dir, "t.csv.gz");
  writeFileSync(
    q,
    gzipSync(
      [
        "exchange,symbol,timestamp,local_timestamp,ask_amount,ask_price,bid_price,bid_amount",
        "coinbase,BTC-USD,1000,1500,1.5,29000.1,29000,2",
        "coinbase,BTC-USD,2000,2500,1.5,29000,29000.05,2",
        "coinbase,BTC-USD,3000,3500,1.5,29000.12,29000.11,2",
      ].join("\n"),
    ),
  );
  writeFileSync(
    t,
    gzipSync(
      [
        "exchange,symbol,timestamp,local_timestamp,id,side,price,amount",
        "coinbase,BTC-USD,1000,1600,1,sell,29000,0.1",
        "coinbase,BTC-USD,1000,1700,2,unknown,29000,0.1",
      ].join("\n"),
    ),
  );
  const Q = await loadQuotes(q);
  assert.equal(Q.stat.kept, 2);
  assert.equal(Q.stat.crossed, 1);
  assert.equal(Q.tick, 0.01);
  assert.equal(Q.s.t[0], 1.5);
  const T = await loadTrades(t);
  assert.equal(T.n, 1);
  assert.equal(T.stat.noSide, 1);
  assert.equal(T.bm[0], 1);
});

test("fees: a maker exit pays two maker fees, a taker exit one of each", () => {
  const F = Object.fromEntries(ORDER_FIELDS.map((k, i) => [k, i]));
  const row = Array(ORDER_FIELDS.length).fill(null);
  row[F.takerGross] = 5;
  row[F.makerGross] = 3;
  row[F.makerHow] = 0;
  const fee = FEES.coinbase;
  assert.deepEqual(nets(row, fee), { takerNet: 5 - 40 - 60, makerNet: 3 - 80 });
  row[F.makerHow] = 1;
  assert.equal(nets(row, fee).makerNet, 3 - 40 - 60);
  row[F.status] = 0;
  row[F.ttfSec] = 30;
  row[F.fillType] = 2;
  const s = summarize([row], fee, () => 0);
  assert.equal(s.fillWithin["1m"], 1);
  assert.equal(s.fillTypeShare.cross, 1);
});

test("days FAITH reserves are refused", () => {
  assert.equal(dayAllowed("2021-01-01"), true);
  assert.equal(dayAllowed("2023-12-01"), true);
  assert.equal(dayAllowed("2024-01-01"), false);
  assert.equal(dayAllowed("2026-03-01"), false);
  assert.equal(TICK.placeToMs + TICK.restMs + 15 * 60_000 <= 86_400_000, true);
});
