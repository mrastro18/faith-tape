// Resting-order (maker) fill simulator on tick-level best bid and ask (Tardis.dev "quotes") and trades.
// Research only: it reads files, places nothing. Derived from FAITH's scripts/loop/queue.mjs (copied here
// as queue.mjs, source commit in its header): the coverage, state and mid helpers (coverAt, rowAt, stateAt,
// midOf, midAt, describe) are used as they are; `simulateTick` and `outcomesTick` are queue.mjs's
// `simulate` and `outcomes` with four changes for tick data:
//   1. Sizes are fresh at every quote row (a tick feed writes a row for every change at the touch), so an
//      order can be placed at any instant, not only at snapshots.
//   2. The book and the trade feed are two streams whose order can disagree by a few milliseconds: the
//      size at the touch may drop before the print that caused it arrives, or a level may vanish before
//      its prints. Prints toward the order at its price within `lookaheadMs` after a quote row are taken
//      as already reflected in that row: the pessimistic cap is size + those prints (so a print is never
//      subtracted twice), the optimistic model does not count them as cancellations, and a level shown
//      empty keeps at most those prints ahead of the order. Without this, a fill could be counted when
//      the same print is subtracted once through the size and once as a print.
//   3. A cross seen in the quotes is labelled by what caused it: if takers' prints toward the order at
//      or through its price arrive within `lookaheadMs`, the fill is "through" (a print beyond the price)
//      or "queue" (prints at the price beyond the queue ahead) rather than "cross".
//   4. Fees are not applied here: outcomes are gross, in bp, and the aggregator applies each venue's fees.
// Fill types: "queue" (takers worked through the queue at the price), "through" (a taker print beyond the
// price), "cross" (the book moved through the order: the opposite quote or an opposite taker print reached
// its price with no taker print toward the order at or through it).
import { createReadStream } from "node:fs";
import { createInterface } from "node:readline";
import { createGunzip } from "node:zlib";
import { coverAt, midAt, midOf, rowAt, stateAt } from "./queue.mjs";

export const MIN = 60_000;
export const TICK = Object.freeze({
  maxGapMs: 120_000, // no quote row for this long breaks coverage (a feed gap or a reconnect)
  latencyMs: 500, // the order is live this long after the instant it was placed on
  lookaheadMs: 250, // feed-order tolerance between book rows and prints (change 2 above)
  restMs: 15 * MIN, // an entry rests at most this long; fill within 1, 5 and 15 minutes are read from it
  fillWithinMin: [1, 5, 15],
  placeEveryMs: MIN, // one placement instant a minute
  placeFromMs: MIN, // ... from 00:01 UTC
  placeToMs: 23 * 60 * MIN + 30 * MIN, // ... to before 23:30 UTC, so 15 min rest and 15 min markout fit the day
  notional: 50, // order size in quote currency (as queue-prov-r1)
  horizonsMin: [1, 5, 15], // mid moves after the fill
  takerExitMin: 5,
  makerExitRestMs: 5 * MIN,
  variants: ["join", "improve"],
  cancels: ["pessimistic", "optimistic"],
  sides: ["buy", "sell"],
});

// ---------------------------------------------------------------------------------------------------------
// Loading.

class Grow {
  constructor(Type = Float64Array, n = 1 << 16) {
    this.Type = Type;
    this.a = new Type(n);
    this.n = 0;
  }
  push(x) {
    if (this.n === this.a.length) {
      const b = new this.Type(this.a.length * 2);
      b.set(this.a);
      this.a = b;
    }
    this.a[this.n++] = x;
  }
  done() {
    return this.a.subarray(0, this.n);
  }
}

async function* csvRows(path) {
  const input = createReadStream(path);
  const stream = path.endsWith(".gz") ? input.pipe(createGunzip()) : input;
  const rl = createInterface({ input: stream, crlfDelay: Infinity });
  let header = null;
  for await (const line of rl) {
    if (!line) continue;
    const f = line.split(",");
    if (!header) {
      header = Object.fromEntries(f.map((k, i) => [k.trim(), i]));
      yield { header };
      continue;
    }
    yield { f };
  }
}

const decimals = (s) => {
  const i = s.indexOf(".");
  if (i < 0 || /e/i.test(s)) return 0;
  let j = s.length;
  while (j > i + 1 && s[j - 1] === "0") j--;
  return j - i - 1;
};

/** Quote rows from an array of {t, bid, bq, ask, aq} (tests and the CSV loader). */
export function quotesFromTicks(rows, cfg = TICK) {
  const n = rows.length;
  const s = {
    t: new Float64Array(n),
    bid: new Float64Array(n),
    bq: new Float64Array(n),
    ask: new Float64Array(n),
    aq: new Float64Array(n),
    s: new Int32Array(n),
  };
  rows.forEach((r, i) => {
    s.t[i] = r.t;
    s.bid[i] = r.bid;
    s.bq[i] = r.bq;
    s.ask[i] = r.ask;
    s.aq[i] = r.aq;
  });
  return finishTicks(s, cfg);
}

function finishTicks(s, cfg) {
  const intervals = [];
  let a = null;
  let prev = null;
  for (let i = 0; i < s.t.length; i++) {
    const t = s.t[i];
    if (prev != null && t - prev > cfg.maxGapMs) {
      intervals.push({ a, b: prev, session: intervals.length });
      a = null;
    }
    if (a == null) a = t;
    s.s[i] = intervals.length;
    prev = t;
  }
  if (a != null) intervals.push({ a, b: prev, session: intervals.length });
  return { q: { intervals }, s };
}

/**
 * Tardis quotes CSV (exchange,symbol,timestamp,local_timestamp,ask_amount,ask_price,bid_price,bid_amount;
 * microseconds). Times are local_timestamp (the recorder's arrival time, one clock for book and trades), in
 * ms. Rows out of time order or with a crossed or locked book are dropped and counted. An empty side is 0.
 */
export async function loadQuotes(path, cfg = TICK) {
  const T = new Grow();
  const B = new Grow();
  const BQ = new Grow();
  const A = new Grow();
  const AQ = new Grow();
  const stat = { rows: 0, kept: 0, crossed: 0, outOfOrder: 0, emptySide: 0, maxDecimals: 0 };
  let h = null;
  let last = -Infinity;
  for await (const r of csvRows(path)) {
    if (r.header) {
      h = r.header;
      continue;
    }
    const f = r.f;
    stat.rows++;
    const t = Number(f[h.local_timestamp]) / 1000;
    const bid = f[h.bid_price] === "" ? 0 : Number(f[h.bid_price]);
    const ask = f[h.ask_price] === "" ? 0 : Number(f[h.ask_price]);
    const bq = f[h.bid_amount] === "" ? 0 : Number(f[h.bid_amount]);
    const aq = f[h.ask_amount] === "" ? 0 : Number(f[h.ask_amount]);
    if (!Number.isFinite(t) || t < last) {
      stat.outOfOrder++;
      continue;
    }
    if (bid > 0 && ask > 0 && ask <= bid) {
      stat.crossed++;
      continue;
    }
    if (!(bid > 0) || !(ask > 0)) stat.emptySide++;
    if (stat.kept < 500_000) {
      if (bid > 0) stat.maxDecimals = Math.max(stat.maxDecimals, decimals(f[h.bid_price]));
      if (ask > 0) stat.maxDecimals = Math.max(stat.maxDecimals, decimals(f[h.ask_price]));
    }
    last = t;
    T.push(t);
    B.push(bid);
    BQ.push(bq);
    A.push(ask);
    AQ.push(aq);
    stat.kept++;
  }
  const s = { t: T.done(), bid: B.done(), bq: BQ.done(), ask: A.done(), aq: AQ.done() };
  s.s = new Int32Array(s.t.length);
  const out = finishTicks(s, cfg);
  out.stat = stat;
  out.tick = 10 ** -stat.maxDecimals;
  return out;
}

/** Trades from an array of {t, p, q, side} where side is the taker's ("buy" | "sell"). */
export function tradesFromTicks(rows) {
  const n = rows.length;
  const tr = {
    n,
    t: new Float64Array(n),
    p: new Float64Array(n),
    q: new Float64Array(n),
    bm: new Uint8Array(n),
  };
  rows.forEach((r, i) => {
    tr.t[i] = r.t;
    tr.p[i] = r.p;
    tr.q[i] = r.q;
    tr.bm[i] = r.side === "sell" ? 1 : 0; // the seller took: the buyer was the maker
  });
  return tr;
}

/**
 * Tardis trades CSV (exchange,symbol,timestamp,local_timestamp,id,side,price,amount); side is the taker's.
 * Times are local_timestamp in ms. Prints with no side, or out of time order, are dropped and counted.
 */
export async function loadTrades(path) {
  const T = new Grow();
  const P = new Grow();
  const Q = new Grow();
  const BM = new Grow(Uint8Array);
  const stat = { rows: 0, kept: 0, noSide: 0, outOfOrder: 0 };
  let h = null;
  let last = -Infinity;
  for await (const r of csvRows(path)) {
    if (r.header) {
      h = r.header;
      continue;
    }
    const f = r.f;
    stat.rows++;
    const t = Number(f[h.local_timestamp]) / 1000;
    const side = f[h.side];
    if (side !== "buy" && side !== "sell") {
      stat.noSide++;
      continue;
    }
    if (!Number.isFinite(t) || t < last) {
      stat.outOfOrder++;
      continue;
    }
    last = t;
    T.push(t);
    P.push(Number(f[h.price]));
    Q.push(Number(f[h.amount]));
    BM.push(side === "sell" ? 1 : 0);
    stat.kept++;
  }
  const t = T.done();
  return { n: t.length, t, p: P.done(), q: Q.done(), bm: BM.done(), stat };
}

// ---------------------------------------------------------------------------------------------------------
// The simulator.

const firstAfter = (arr, t) => {
  let lo = 0;
  let hi = arr.length;
  while (lo < hi) {
    const m = (lo + hi) >> 1;
    if (arr[m] <= t) lo = m + 1;
    else hi = m;
  }
  return lo;
};

/**
 * One resting order on tick data. `o`: {side, t0, price, qty, cancels}. `ctx`: {q, s, tr, tick, restMs,
 * latencyMs, lookaheadMs}. Returns {status, ...} as queue.mjs `simulate`; a fill also carries `adj` (the
 * feed-order tolerance changed the queue or the label for this order).
 */
export function simulateTick(o, ctx) {
  const { q, s, tr, tick } = ctx;
  const restMs = ctx.restMs ?? TICK.restMs;
  const latencyMs = ctx.latencyMs ?? TICK.latencyMs;
  const la = ctx.lookaheadMs ?? TICK.lookaheadMs;
  const tk = (p) => Math.round(p / tick);
  const buy = o.side === "buy";
  const bidT = (p) => (p > 0 ? tk(p) : -Infinity);
  const askT = (p) => (p > 0 ? tk(p) : Infinity);
  const st = stateAt(q, s, o.t0);
  if (!st) return { status: "no_state" };
  const P = tk(o.price);
  const near = buy ? bidT(st.bid) : askT(st.ask);
  const far = buy ? askT(st.ask) : bidT(st.bid);
  const better = (x, y) => (buy ? x > y : x < y);
  if (!better(far, P)) return { status: "would_cross", P };
  let ahead;
  if (better(P, near)) ahead = 0;
  else if (P === near) ahead = buy ? st.bq : st.aq;
  else return { status: "not_modelled", P };
  const ahead0 = ahead;
  let lastSize = ahead;
  let traded = 0; // takers' quantity at P since the last look, not credited to a quote row ahead of time
  let filled = 0;
  let pre = 0; // prints with index below this were credited to an earlier quote row (optimistic)
  let adj = false;
  const horizon = o.t0 + restMs;
  const stop = Math.min(horizon, st.iv.b);
  const live = o.t0 + latencyMs;
  let ti = firstAfter(tr.t, o.t0);
  let qi = st.i + 1;
  const toward = (j) => (buy ? tr.bm[j] === 1 : tr.bm[j] === 0);
  // prints toward the order not yet processed, within la after a quote row at time tq
  const pending = (tq) => {
    let atP = 0;
    let atPNew = 0;
    let through = false;
    let j = ti;
    for (; j < tr.n && tr.t[j] <= tq + la; j++) {
      if (!toward(j)) continue;
      const xp = tk(tr.p[j]);
      if (xp === P) {
        atP += tr.q[j];
        if (j >= pre) atPNew += tr.q[j];
      } else if (better(P, xp)) through = true;
    }
    return { atP, atPNew, through, end: j };
  };
  const done = (t, type) => ({
    status: "filled",
    P,
    ahead0,
    fillT: t,
    fillType: type,
    filledQty: o.qty,
    adj,
  });
  for (;;) {
    const tT = ti < tr.n ? tr.t[ti] : Infinity;
    const tQ = qi < s.t.length ? s.t[qi] : Infinity;
    const t = Math.min(tT, tQ);
    if (t > stop) break;
    const isLive = t >= live;
    if (tT <= tQ) {
      const j = ti++;
      const xp = tk(tr.p[j]);
      const x = tr.q[j];
      if (toward(j)) {
        if (better(P, xp)) {
          if (isLive) return done(t, "through");
          ahead = 0;
          lastSize = 0;
          traded = 0;
        } else if (xp === P) {
          if (j >= pre) traded += x;
          if (isLive) {
            const rest = x - ahead;
            if (rest > 0) filled += rest;
            if (filled >= o.qty - 1e-12) return done(t, "queue");
          }
          ahead = Math.max(0, ahead - x);
        }
      } else if (!better(xp, P)) {
        if (isLive) return done(t, "cross");
        return { status: "would_cross", P, late: true };
      }
    } else {
      const i = qi++;
      const n = buy ? bidT(s.bid[i]) : askT(s.ask[i]);
      const f = buy ? askT(s.ask[i]) : bidT(s.bid[i]);
      const size = buy ? s.bq[i] : s.aq[i];
      if (!better(f, P)) {
        if (!isLive) return { status: "would_cross", P, late: true };
        const pd = pending(s.t[i]);
        if (pd.through) {
          adj = true;
          return done(s.t[i], "through");
        }
        if (pd.atP > ahead + 1e-12) {
          adj = true;
          return done(s.t[i], "queue");
        }
        return done(s.t[i], "cross");
      }
      if (better(P, n)) {
        // the level at P is shown empty; prints at P still to arrive were ahead of the order at most
        const pd = pending(s.t[i]);
        if (pd.atP > 0) {
          if (pd.atP < ahead) adj = true;
          ahead = Math.min(ahead, pd.atP);
        } else ahead = 0;
        lastSize = 0;
        traded = 0;
        pre = Math.max(pre, pd.end);
      } else if (n === P) {
        const pd = pending(s.t[i]);
        if (o.cancels === "optimistic") {
          const dec = lastSize - traded - pd.atPNew - size;
          if (dec > 0) ahead = Math.max(0, ahead - dec);
          pre = Math.max(pre, pd.end);
        }
        const cap = size + pd.atP;
        if (cap < ahead) {
          if (size < ahead && pd.atP > 0) adj = true;
          ahead = cap;
        }
        lastSize = size;
        traded = 0;
      }
    }
  }
  if (stop >= horizon) return { status: "expired", P, ahead0, filledQty: filled };
  return { status: "unknown", P, ahead0, filledQty: filled };
}

/**
 * What a fill earned, signed for the position (+ is good), gross of fees, in bp: half-spread at the fill
 * (edge0), the mid's move from the fill to each horizon (midMove: adverse selection when negative), price
 * against the mid at each horizon (edge), a taker exit at the touch after takerExitMin (takerGross), and a
 * maker exit: rest the opposite side by the same variant rule for makerExitRestMs, else a taker exit at the
 * touch then (maker {how, gross}; how is "maker", "taker", "cross" or "unknown").
 */
export function outcomesTick(fill, o, ctx, cfg = TICK) {
  const { q, s, tick } = ctx;
  const sg = o.side === "buy" ? 1 : -1;
  const P = fill.P * tick;
  const st0 = stateAt(q, s, fill.fillT);
  const out = { edge0: null, midMove: {}, edge: {}, takerGross: null, maker: { how: "unknown" } };
  const m0 = st0 ? midOf(st0) : null;
  if (m0 != null) out.edge0 = (sg * (m0 - P) * 1e4) / P;
  for (const h of cfg.horizonsMin) {
    const m = midAt(q, s, fill.fillT + h * MIN);
    out.midMove[h] = m != null && m0 != null ? (sg * (m - m0) * 1e4) / m0 : null;
    out.edge[h] = m != null ? (sg * (m - P) * 1e4) / P : null;
  }
  const takerAt = (t) => {
    const st = stateAt(q, s, t);
    if (!st || !(st.bid > 0 && st.ask > 0)) return null;
    const px = o.side === "buy" ? st.bid : st.ask;
    return (sg * (px - P) * 1e4) / P;
  };
  out.takerGross = takerAt(fill.fillT + cfg.takerExitMin * MIN);
  if (st0 && m0 != null) {
    const exitSide = o.side === "buy" ? "sell" : "buy";
    const touch = exitSide === "sell" ? st0.ask : st0.bid;
    const room = Math.round((st0.ask - st0.bid) / tick) >= 2;
    const px =
      o.variant === "improve" && room ? touch + (exitSide === "sell" ? -tick : tick) : touch;
    const r = simulateTick(
      { side: exitSide, t0: fill.fillT, price: px, qty: o.qty, cancels: o.cancels },
      { ...ctx, restMs: cfg.makerExitRestMs },
    );
    if (r.status === "filled")
      out.maker = { how: "maker", gross: (sg * (r.P * tick - P) * 1e4) / P };
    else if (r.status === "expired" || r.status === "would_cross") {
      const g = takerAt(r.status === "expired" ? fill.fillT + cfg.makerExitRestMs : fill.fillT);
      out.maker =
        g != null ? { how: r.status === "expired" ? "taker" : "cross", gross: g } : { how: "unknown" };
    }
  }
  return out;
}

// ---------------------------------------------------------------------------------------------------------
// One day of one pair: every placement, as compact rows for the aggregator.

export const STATUS = ["filled", "expired", "expiredPartial", "unknown", "noRoom", "would_cross", "not_modelled", "no_state"];
export const FILL = ["queue", "through", "cross"];
export const HOW = ["maker", "taker", "cross", "unknown"];
export const ORDER_FIELDS = [
  "variant", "cancels", "side", "status", "ttfSec", "fillType", "ahead0Quote", "spreadTicks", "spreadBp",
  "edge0", "mm1", "mm5", "mm15", "e1", "e5", "e15", "takerGross", "makerHow", "makerGross", "adj",
];
export const CONTROL_FIELDS = ["side", "c1", "c5", "c15"];

const r4 = (x) => (x == null || !Number.isFinite(x) ? null : Math.round(x * 1e4) / 1e4);

/**
 * Simulates every placement of one UTC day. Returns {orders: rows of ORDER_FIELDS, control: rows of
 * CONTROL_FIELDS, calibration, placements}. Indices into TICK.variants, cancels, sides, STATUS, FILL, HOW.
 */
export function runDay({ q, s, tr, tick, dayStartMs, cfg = TICK }) {
  const ctx = { q, s, tr, tick, restMs: cfg.restMs, latencyMs: cfg.latencyMs, lookaheadMs: cfg.lookaheadMs };
  const orders = [];
  const control = [];
  let placements = 0;
  let noBook = 0;
  for (let t0 = dayStartMs + cfg.placeFromMs; t0 < dayStartMs + cfg.placeToMs; t0 += cfg.placeEveryMs) {
    const st = stateAt(q, s, t0);
    if (!st || !(st.bid > 0) || !(st.ask > st.bid)) {
      noBook++;
      continue;
    }
    placements++;
    const spreadTicks = Math.round((st.ask - st.bid) / tick);
    const m0 = midOf(st);
    const spreadBp = ((st.ask - st.bid) * 1e4) / m0;
    cfg.sides.forEach((side, si) => {
      const sg = side === "buy" ? 1 : -1;
      const c = [si];
      for (const h of cfg.horizonsMin) {
        const m = midAt(q, s, t0 + h * MIN);
        c.push(m != null ? r4((sg * (m - m0) * 1e4) / m0) : null);
      }
      control.push(c);
      cfg.variants.forEach((variant, vi) => {
        const touch = side === "buy" ? st.bid : st.ask;
        let price = touch;
        if (variant === "improve") {
          if (spreadTicks < 2) {
            cfg.cancels.forEach((_, ci) =>
              orders.push([vi, ci, si, STATUS.indexOf("noRoom"), ...Array(16).fill(null)]),
            );
            return;
          }
          price = touch + (side === "buy" ? tick : -tick);
        }
        const qty = cfg.notional / price;
        cfg.cancels.forEach((cancels, ci) => {
          const o = { side, t0, price, qty, cancels, variant };
          const r = simulateTick(o, ctx);
          const status = r.status === "expired" && r.filledQty > 0 ? "expiredPartial" : r.status;
          const row = [vi, ci, si, STATUS.indexOf(status)];
          if (r.status !== "filled") {
            row.push(null, null, r.ahead0 != null ? r4(r.ahead0 * price) : null, spreadTicks, r4(spreadBp));
            row.push(...Array(11).fill(null));
            orders.push(row);
            return;
          }
          const out = outcomesTick(r, o, ctx, cfg);
          row.push(
            r4((r.fillT - t0) / 1000),
            FILL.indexOf(r.fillType),
            r4(r.ahead0 * price),
            spreadTicks,
            r4(spreadBp),
            r4(out.edge0),
            ...cfg.horizonsMin.map((h) => r4(out.midMove[h])),
            ...cfg.horizonsMin.map((h) => r4(out.edge[h])),
            r4(out.takerGross),
            HOW.indexOf(out.maker.how),
            r4(out.maker.gross ?? null),
            r.adj ? 1 : 0,
          );
          orders.push(row);
        });
      });
    });
  }
  return { orders, control, placements, noBook, calibration: calibrateTicks(q, s, tr, tick) };
}

/**
 * Each print against the last quote strictly before it: at the touch on its side, through it, inside the
 * spread, or at or past the other side. A high inside or past share means the book leads the prints.
 */
export function calibrateTicks(q, s, tr, tick) {
  const tk = (p) => Math.round(p / tick);
  const out = { bid: {}, ask: {} };
  const add = (o, k) => (o[k] = (o[k] ?? 0) + 1);
  for (let j = 0; j < tr.n; j++) {
    const t = tr.t[j];
    const iv = coverAt(q.intervals, t);
    if (!iv) continue;
    const i = rowAt(s, t - 1e-6, iv.session);
    if (i < 0 || !(s.bid[i] > 0 && s.ask[i] > 0)) continue;
    const side = tr.bm[j] === 1 ? "bid" : "ask";
    const b = tk(s.bid[i]);
    const a = tk(s.ask[i]);
    const xp = tk(tr.p[j]);
    const touch = side === "bid" ? b : a;
    const other = side === "bid" ? a : b;
    const inner = side === "bid" ? xp > touch : xp < touch;
    let cls;
    if (xp === touch) cls = "atTouch";
    else if (inner && (side === "bid" ? xp >= other : xp <= other)) cls = "atOrPastOtherSide";
    else if (inner) cls = "insideSpread";
    else cls = "throughTouch";
    add(out[side], cls);
  }
  return out;
}
