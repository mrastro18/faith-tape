// Copied unchanged from FAITH (github.com/mrastro18/faith) scripts/loop/queue.mjs at commit b801165a0eec3203a7471eab7f60ac11d3a0bc44
// (branch claude/queue-sim). Only these two header lines are added. Do not edit here: change FAITH and copy again.
// Resting-order (maker) fill simulator on recorded Binance.US best bid and ask (faith-tape quotes) and the
// aggregate-trade tape. Research only: it reads files, places nothing.
//
// A resting limit buy placed at time t0 at price P (a sell is the mirror):
//   - P at or above the best ask is refused (post-only): "would_cross".
//   - P above the best bid improves the price: nobody is ahead, queue 0.
//   - P equal to the best bid joins it: the displayed size there is ahead.
//   - P below the best bid sits at a deeper level whose size is not recorded: "not_modelled", excluded.
// The queue ahead is consumed by seller-taker prints at P; once it is gone, further prints at P fill the
// order. A seller-taker print below P (through it) fills the rest. A sell offered at or below P (a quote
// with the ask at or below P, or a buyer-taker print at or below P) would have traded with the order:
// filled, type "cross". The observed book never contains the simulated order, so a displayed bid below P
// means the level at P was empty: nobody is ahead any more.
// Cancellations ahead, two models: "pessimistic" (nobody ahead cancels: the queue falls only by prints at
// P, and never stays above the displayed size there) and "optimistic" (any fall in the displayed size at P
// not explained by prints at P since the last look came from ahead of the order).
// Quotes are known only inside recorded sessions, at the 5 s polls, and only up to a failed poll ("x"), a
// silence longer than `maxPollGapMs`, or the session's byte cap ("c"). An order still resting when its
// stretch of known quotes ends is "unknown", never filled: prints after that point are not used for it.
// A size change alone is not a quote row (faith-tape README), so the displayed size is fresh only at a
// poll that wrote a row for the pair (a price change on either side, or the 10-minute snapshot).
// Spec queue-prov-r1 runs it once on the tape as recorded; see runQueueProv.
import { execFileSync } from "node:child_process";
import { existsSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { gunzipSync } from "node:zlib";
import { clusterBootstrap, quantile } from "./stats.mjs";
import { trades as readTrades } from "./tape.mjs";

const MIN = 60_000;
export const QUEUE = Object.freeze({
  maxPollGapMs: 12_000, // polls are 5 s apart; a longer silence inside a session breaks coverage
  latencyMs: 500, // the order is live this long after the quote it was placed on arrived
  restMs: 5 * MIN, // an entry rests at most this long
  notional: 50, // order size in quote currency
  horizonsMin: [1, 5, 15], // mid moves after the fill
  takerExitMin: 5, // taker exit at the touch this long after the fill
  makerExitRestMs: 5 * MIN, // a maker exit rests this long, then exits as a taker at the touch
  takerFeeBp: 2, // Binance.US 0.02% (09-venue-facts)
  makerFeeBp: 0,
  stableBases: ["USDT", "USDC", "USD1", "TUSD", "FDUSD", "DAI", "PYUSD", "USDP", "BUSD"],
  variants: ["join", "improve"],
  cancels: ["pessimistic", "optimistic"],
  sides: ["buy", "sell"],
  summaryMinClusters: 10, // descriptive intervals over UTC hours; not inference (stats.MIN_CLUSTERS is 30 days)
  resamples: 2000,
});

// ---------------------------------------------------------------------------------------------------------
// Quotes: sessions, per-pair series, coverage.

/** A builder fed quote rows in file order; `done()` returns {sessions, series, intervals, polls}. */
export function quoteBuilder(cfg = QUEUE) {
  const sessions = [];
  const series = new Map();
  let cur = null;
  const ser = (sym) => {
    let s = series.get(sym);
    if (!s) {
      s = { t: [], bid: [], bq: [], ask: [], aq: [], s: [], full: [] };
      series.set(sym, s);
    }
    return s;
  };
  const close = () => {
    if (cur) {
      if (cur.end == null) cur.end = cur.polls.at(-1) ?? cur.start;
      sessions.push(cur);
    }
    cur = null;
  };
  return {
    /** A poll seen through a row of a pair that is not kept. */
    poll(t) {
      if (cur && cur.polls.at(-1) !== t) cur.polls.push(t);
    },
    add(r) {
      const k = r[0];
      if (k === "s") {
        close();
        cur = { start: r[1], end: null, ended: false, polls: [], snaps: [], xs: [], cap: null };
        return;
      }
      if (!cur) return; // rows before any session start are not trusted
      const t = r[1];
      if (k === "f" || k === "q") {
        if (cur.polls.at(-1) !== t) cur.polls.push(t);
        if (k === "f" && cur.snaps.at(-1) !== t) cur.snaps.push(t);
        const s = ser(r[2]);
        s.t.push(t);
        s.bid.push(r[3]);
        s.bq.push(r[4]);
        s.ask.push(r[5]);
        s.aq.push(r[6]);
        s.s.push(sessions.length);
        s.full.push(k === "f");
      } else if (k === "x") cur.xs.push(t);
      else if (k === "c") cur.cap = cur.cap ?? t;
      else if (k === "e") {
        cur.end = t;
        cur.ended = true;
        close();
      }
    },
    done() {
      close();
      return finishQuotes(sessions, series, cfg);
    },
  };
}

function finishQuotes(sessions, series, cfg) {
  const intervals = [];
  sessions.forEach((ss, si) => {
    let a = null;
    let prev = null;
    let xi = 0;
    for (const p of ss.polls) {
      if (ss.cap != null && p >= ss.cap) break; // after the cap only snapshots: changes unknown
      let brk = prev != null && p - prev > cfg.maxPollGapMs;
      while (xi < ss.xs.length && ss.xs[xi] <= p) {
        if (prev != null && ss.xs[xi] > prev) brk = true;
        xi++;
      }
      if (brk) {
        intervals.push({ a, b: prev, session: si });
        a = null;
      }
      if (a == null) a = p;
      prev = p;
    }
    if (a != null) intervals.push({ a, b: prev, session: si });
  });
  intervals.sort((x, y) => x.a - y.a);
  const polls = sessions.flatMap((s) => s.polls).sort((x, y) => x - y);
  return { sessions, series, intervals, polls };
}

/** Quotes from an array of rows (tests). */
export function quotesFromRows(rows, cfg = QUEUE) {
  const b = quoteBuilder(cfg);
  for (const r of rows) b.add(r);
  return b.done();
}

/**
 * Quote files for sessions that start in [fromMs, toMs): data/binanceus/quotes/<day>.ndjson (before the
 * per-session format) and quotes/<day>/<HHMMSS>Z-<run>.ndjson[.gz]. Only `symbols` (a Set) are kept, if given.
 */
export function readQuotes(root, fromMs, toMs, { symbols = null, cfg = QUEUE } = {}) {
  const dir = join(root, "data", "binanceus", "quotes");
  const files = [];
  if (existsSync(dir))
    for (const d of readdirSync(dir).sort()) {
      if (!/^\d{4}-\d{2}-\d{2}/.test(d)) continue;
      const day = Date.parse(d.slice(0, 10) + "T00:00:00Z");
      if (day + 86_400_000 <= fromMs || day >= toMs) continue;
      if (d.endsWith(".ndjson")) files.push(join(dir, d));
      else
        for (const f of readdirSync(join(dir, d)).sort())
          if (/\.ndjson(\.gz)?$/.test(f)) files.push(join(dir, d, f));
    }
  const parsed = [];
  for (const f of files) {
    const buf = readFileSync(f);
    const text = f.endsWith(".gz") ? gunzipSync(buf).toString("utf8") : buf.toString("utf8");
    let first = null;
    const rows = [];
    for (const line of text.split("\n")) {
      if (!line) continue;
      const r = JSON.parse(line);
      if (first == null) first = r[1];
      if ((r[0] === "f" || r[0] === "q") && symbols && !symbols.has(r[2])) rows.push(["p", r[1]]);
      else rows.push(r);
    }
    parsed.push({ first: first ?? 0, rows });
  }
  parsed.sort((x, y) => x.first - y.first);
  const b = quoteBuilder(cfg);
  let keep = false;
  for (const { rows } of parsed)
    for (const r of rows) {
      if (r[0] === "s") keep = r[1] >= fromMs && r[1] < toMs;
      if (keep) {
        if (r[0] === "p") b.poll(r[1]);
        else b.add(r);
      }
      if (r[0] === "e") keep = false;
    }
  return b.done();
}

/** The coverage interval holding t (a <= t <= b), or null. */
export function coverAt(intervals, t) {
  let lo = 0;
  let hi = intervals.length - 1;
  let ans = -1;
  while (lo <= hi) {
    const m = (lo + hi) >> 1;
    if (intervals[m].a <= t) {
      ans = m;
      lo = m + 1;
    } else hi = m - 1;
  }
  if (ans < 0) return null;
  const iv = intervals[ans];
  return t <= iv.b ? iv : null;
}

/** Index of the last row of `s` at or before t, in session `session` (else -1). */
export function rowAt(s, t, session) {
  let lo = 0;
  let hi = s.t.length - 1;
  let ans = -1;
  while (lo <= hi) {
    const m = (lo + hi) >> 1;
    if (s.t[m] <= t) {
      ans = m;
      lo = m + 1;
    } else hi = m - 1;
  }
  return ans >= 0 && s.s[ans] === session ? ans : -1;
}

/** The best bid and ask known at t ({bid, bq, ask, aq, i, fresh}), or null outside coverage. */
export function stateAt(q, s, t) {
  const iv = coverAt(q.intervals, t);
  if (!iv || !s) return null;
  const i = rowAt(s, t, iv.session);
  if (i < 0) return null;
  return { bid: s.bid[i], bq: s.bq[i], ask: s.ask[i], aq: s.aq[i], i, fresh: s.t[i] === t, iv };
}

/** Mid of a quote, or null when a side is empty (Binance.US reports an empty side as price 0). */
export const midOf = (st) => (st.bid > 0 && st.ask > 0 ? (st.bid + st.ask) / 2 : null);

// ---------------------------------------------------------------------------------------------------------
// The simulator.

const firstAfter = (arr, t, key = (x) => x) => {
  let lo = 0;
  let hi = arr.length;
  while (lo < hi) {
    const m = (lo + hi) >> 1;
    if (key(arr[m]) <= t) lo = m + 1;
    else hi = m;
  }
  return lo;
};

/**
 * One resting order. `o`: {side "buy"|"sell", t0, price, qty (base units), cancels}. `ctx`: {q (quotes),
 * s (the pair's series), tr (the pair's trades {t, p, q, buyerMaker}, sorted by time), tick, restMs,
 * latencyMs}. Prices are compared in whole ticks. Returns {status, ...}: status is "filled", "expired"
 * (rested the full time in known quotes; filledQty may be partial), "unknown" (known quotes ended first),
 * "would_cross", "not_modelled" or "no_state".
 */
export function simulate(o, ctx) {
  const { q, s, tr, tick } = ctx;
  const restMs = ctx.restMs ?? QUEUE.restMs;
  const latencyMs = ctx.latencyMs ?? QUEUE.latencyMs;
  const tk = (p) => Math.round(p / tick);
  const buy = o.side === "buy";
  // an empty side (price 0) is no price at all
  const bidT = (p) => (p > 0 ? tk(p) : -Infinity);
  const askT = (p) => (p > 0 ? tk(p) : Infinity);
  const st = stateAt(q, s, o.t0);
  if (!st) return { status: "no_state" };
  const P = tk(o.price);
  const near = buy ? bidT(st.bid) : askT(st.ask); // our side's best
  const far = buy ? askT(st.ask) : bidT(st.bid); // the other side's best
  const better = (x, y) => (buy ? x > y : x < y); // x is a better price than y for our side
  if (!better(far, P)) return { status: "would_cross", P };
  let ahead;
  if (better(P, near)) ahead = 0;
  else if (P === near) ahead = buy ? st.bq : st.aq;
  else return { status: "not_modelled", P };
  const ahead0 = ahead;
  let lastSize = ahead; // last displayed size seen at P
  let traded = 0; // takers' quantity at P since that look
  let filled = 0;
  const horizon = o.t0 + restMs;
  const stop = Math.min(horizon, st.iv.b);
  const live = o.t0 + latencyMs;
  let ti = firstAfter(tr, o.t0, (x) => x.t);
  let qi = st.i + 1;
  const done = (t, type) => ({
    status: "filled",
    P,
    ahead0,
    fillT: t,
    fillType: type,
    filledQty: o.qty,
  });
  for (;;) {
    const tT = ti < tr.length ? tr[ti].t : Infinity;
    const tQ = qi < s.t.length ? s.t[qi] : Infinity;
    const t = Math.min(tT, tQ);
    if (t > stop) break;
    const isLive = t >= live;
    if (tT <= tQ) {
      const x = tr[ti++];
      const xp = tk(x.p);
      const takerSells = x.buyerMaker; // the buyer rested, so the seller took
      const towardUs = buy ? takerSells : !takerSells; // a taker hitting our side of the book
      if (towardUs) {
        if (better(P, xp)) {
          // through our price: the level at P is gone
          if (isLive) return done(x.t, "through");
          ahead = 0;
          lastSize = 0;
          traded = 0;
        } else if (xp === P) {
          traded += x.q;
          if (isLive) {
            const rest = x.q - ahead;
            if (rest > 0) filled += rest;
            if (filled >= o.qty - 1e-12) return done(x.t, "queue");
          }
          ahead = Math.max(0, ahead - x.q);
        }
      } else if (!better(xp, P)) {
        // a resting offer on the other side at or through our price
        if (isLive) return done(x.t, "cross");
        return { status: "would_cross", P, late: true };
      }
    } else {
      const i = qi++;
      const n = buy ? bidT(s.bid[i]) : askT(s.ask[i]);
      const f = buy ? askT(s.ask[i]) : bidT(s.bid[i]);
      const size = buy ? s.bq[i] : s.aq[i];
      if (!better(f, P)) {
        if (isLive) return done(s.t[i], "cross");
        return { status: "would_cross", P, late: true };
      }
      if (better(P, n)) {
        ahead = 0;
        lastSize = 0;
        traded = 0;
      } else if (n === P) {
        if (o.cancels === "optimistic") {
          const dec = lastSize - traded - size;
          if (dec > 0) ahead = Math.max(0, ahead - dec);
        }
        ahead = Math.min(ahead, size);
        lastSize = size;
        traded = 0;
      }
    }
  }
  if (stop >= horizon) return { status: "expired", P, ahead0, filledQty: filled };
  return { status: "unknown", P, ahead0, filledQty: filled, coverEnd: st.iv.b };
}

/** Mid at t if quotes are known then, else null. */
export function midAt(q, s, t) {
  const st = stateAt(q, s, t);
  return st ? midOf(st) : null;
}

/**
 * What a fill earned, signed for the position (+ is good), in bp: half-spread captured at the fill
 * (edge0), the mid's move from the fill to each horizon (midMove), price against the mid at each horizon
 * (edge), a taker exit at the touch after `takerExitMin` net of the fee, and a maker exit: rest the
 * opposite side at the same rule for `makerExitRestMs`, else exit as a taker at the touch.
 */
export function outcomes(fill, o, ctx, cfg = QUEUE) {
  const { q, s, tick } = ctx;
  const sg = o.side === "buy" ? 1 : -1;
  const P = fill.P * tick;
  const st0 = stateAt(q, s, fill.fillT);
  const out = { edge0: null, midMove: {}, edge: {}, takerExit: null, takerThin: null, maker: null };
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
    const size = o.side === "buy" ? st.bq : st.aq;
    return { net: (sg * (px - P) * 1e4) / P - cfg.takerFeeBp, thin: size * px < o.qty * P };
  };
  const tx = takerAt(fill.fillT + cfg.takerExitMin * MIN);
  if (tx) {
    out.takerExit = tx.net;
    out.takerThin = tx.thin;
  }
  // Maker exit, placed when the fill is known (the order is live latencyMs later).
  if (st0 && m0 != null) {
    const exitSide = o.side === "buy" ? "sell" : "buy";
    const touch = exitSide === "sell" ? st0.ask : st0.bid;
    const room = Math.round((st0.ask - st0.bid) / tick) >= 2;
    const px =
      o.variant === "improve" && room ? touch + (exitSide === "sell" ? -tick : tick) : touch;
    const r = simulate(
      { side: exitSide, t0: fill.fillT, price: px, qty: o.qty, cancels: o.cancels },
      { ...ctx, restMs: cfg.makerExitRestMs },
    );
    if (r.status === "filled")
      out.maker = { how: "maker", net: (sg * (r.P * tick - P) * 1e4) / P - cfg.makerFeeBp * 2 };
    else if (r.status === "expired" || r.status === "would_cross") {
      const t = r.status === "expired" ? fill.fillT + cfg.makerExitRestMs : fill.fillT;
      const x = takerAt(t);
      out.maker = x
        ? { how: r.status === "expired" ? "taker" : "cross", net: x.net }
        : { how: "unknown" };
    } else out.maker = { how: "unknown" };
  } else out.maker = { how: "unknown" };
  return out;
}

// ---------------------------------------------------------------------------------------------------------
// Calibration: do the quotes and the prints agree with the simulator's assumptions?

/**
 * For one pair, both books. A taker print is classed against the last quote strictly before it. Prints are
 * grouped by the poll interval (p_i, p_i+1] they fall in, inside coverage, and the group compared with the
 * quote at p_i and at p_i+1: when takers took at least the displayed size at the touch, is the level gone
 * at the next poll? When they took less and the next poll shows the same price with a fresh size, did the
 * size fall by at least what was taken? Snapshots of an unchanged price show how often the size changed
 * without a row.
 */
export function calibratePair(q, s, tr, tick, acc) {
  const tk = (p) => Math.round(p / tick);
  const polls = q.polls;
  const add = (o, k, n = 1) => (o[k] = (o[k] ?? 0) + n);
  for (const side of ["bid", "ask"]) {
    const A = (acc[side] ??= {
      prints: {},
      depletion: {},
      flow: {},
      insideNextTouch: 0,
      inside: 0,
    });
    const groups = new Map();
    for (const x of tr) {
      const toBid = x.buyerMaker; // seller took: hits the bid
      if ((side === "bid") !== toBid) continue;
      const iv = coverAt(q.intervals, x.t);
      if (!iv || x.t === iv.a) continue;
      const i = rowAt(s, x.t - 1, iv.session);
      if (i < 0 || !(s.bid[i] > 0 && s.ask[i] > 0)) continue;
      const b = tk(s.bid[i]);
      const a = tk(s.ask[i]);
      const xp = tk(x.p);
      const touch = side === "bid" ? b : a;
      const other = side === "bid" ? a : b;
      const inner = side === "bid" ? xp > touch : xp < touch; // better than our side's touch
      let cls;
      if (xp === touch) cls = "atTouch";
      else if (inner && (side === "bid" ? xp >= other : xp <= other)) cls = "atOrPastOtherSide";
      else if (inner) cls = "insideSpread";
      else cls = "throughTouch";
      add(A.prints, cls);
      // the poll interval holding the print
      const pi = firstAfter(polls, x.t - 1) - 1; // last poll < x.t
      const p0 = polls[pi];
      const p1 = polls[pi + 1];
      if (p0 == null || p1 == null || p0 < iv.a || p1 > iv.b) continue;
      if (cls === "insideSpread") {
        A.inside++;
        const j = rowAt(s, p1, iv.session);
        if (j >= 0 && tk(side === "bid" ? s.bid[j] : s.ask[j]) === xp) A.insideNextTouch++;
      }
      const j0 = rowAt(s, p0, iv.session);
      if (j0 < 0) continue;
      const key = pi;
      let g = groups.get(key);
      if (!g) {
        g = { p0, p1, j0, session: iv.session, atQty: 0, through: false };
        groups.set(key, g);
      }
      const t0 = tk(side === "bid" ? s.bid[j0] : s.ask[j0]);
      if (xp === t0) g.atQty += x.q;
      else if (side === "bid" ? xp < t0 : xp > t0) g.through = true;
    }
    for (const g of groups.values()) {
      const j1 = rowAt(s, g.p1, g.session);
      if (j1 < 0 || !(s.bid[j1] > 0 && s.ask[j1] > 0)) continue;
      const t0 = tk(side === "bid" ? s.bid[g.j0] : s.ask[g.j0]);
      const t1 = tk(side === "bid" ? s.bid[j1] : s.ask[j1]);
      const sz0 = side === "bid" ? s.bq[g.j0] : s.aq[g.j0];
      const sz1 = side === "bid" ? s.bq[j1] : s.aq[j1];
      const fresh0 = s.t[g.j0] === g.p0;
      const worse = side === "bid" ? t1 < t0 : t1 > t0;
      const next = worse ? "goneOrWorse" : t1 === t0 ? "samePrice" : "better";
      if (g.through) add(A.depletion, `through|${next}`);
      if (g.atQty <= 0) continue;
      const exceeds = g.atQty >= sz0 - 1e-12;
      add(
        A.depletion,
        `${exceeds ? "takenAtLeastShown" : "takenLessThanShown"}|${fresh0 ? "fresh" : "stale"}|${next}`,
      );
      if (!exceeds && fresh0 && next === "samePrice" && s.t[j1] === g.p1) {
        const dec = sz0 - sz1;
        add(A.flow, "n");
        if (dec >= g.atQty - 1e-12) add(A.flow, dec > g.atQty + 1e-12 ? "fellMore" : "fellExactly");
        else if (dec >= 0) add(A.flow, "fellLess");
        else add(A.flow, "grew");
      }
    }
  }
  // Size changes without a row: a snapshot whose prices equal the pair's previous row in the session.
  const Z = (acc.silent ??= { n: 0, bidSizeChanged: 0, askSizeChanged: 0, relChange: [] });
  for (let i = 1; i < s.t.length; i++) {
    if (!s.full[i] || s.s[i] !== s.s[i - 1]) continue;
    if (s.bid[i] !== s.bid[i - 1] || s.ask[i] !== s.ask[i - 1]) continue;
    Z.n++;
    if (s.bq[i] !== s.bq[i - 1]) {
      Z.bidSizeChanged++;
      if (s.bq[i - 1] > 0) Z.relChange.push(Math.abs(s.bq[i] - s.bq[i - 1]) / s.bq[i - 1]);
    }
    if (s.aq[i] !== s.aq[i - 1]) Z.askSizeChanged++;
  }
  return acc;
}

// ---------------------------------------------------------------------------------------------------------
// The provisional read (spec queue-prov-r1).

const pct = (xs, p) => {
  if (!xs.length) return null;
  const a = [...xs].sort((x, y) => x - y);
  return quantile(a, p);
};
const share = (a, b) => (b > 0 ? a / b : null);

/** Mean with a descriptive 90% interval over UTC-hour clusters (not inference: too few days). */
export function describe(items, cfg = QUEUE) {
  const ok = items.filter((x) => Number.isFinite(x.value));
  if (!ok.length) return { n: 0 };
  const b = clusterBootstrap(ok, {
    resamples: cfg.resamples,
    minClusters: cfg.summaryMinClusters,
  });
  const vals = ok.map((x) => x.value);
  return {
    n: ok.length,
    mean: b.mean,
    median: pct(vals, 0.5),
    lo90: Number.isFinite(b.lo) ? b.lo : null,
    hi90: Number.isFinite(b.hi) ? b.hi : null,
    hours: b.clusters,
  };
}

/** Universe: recorded pairs (state universe "in"), stablecoin bases out, with a tick size. */
export function universe(tapeRoot, cfg = QUEUE) {
  const st = JSON.parse(readFileSync(join(tapeRoot, "state", "state.json"), "utf8"));
  const b = st.binanceus ?? {};
  const u = b.universe ?? {};
  const stable = new Set(cfg.stableBases);
  const out = [];
  const dropped = { notIn: 0, stable: [], noTick: 0, notCaughtUp: [] };
  for (const p of b.pairs ?? []) {
    if (!u[p.symbol]?.in) {
      dropped.notIn++;
      continue;
    }
    if (stable.has(p.base)) {
      dropped.stable.push(p.symbol);
      continue;
    }
    if (!(Number(p.tick) > 0)) {
      dropped.noTick++;
      continue;
    }
    const c = b.cursors?.[p.symbol];
    if (c && c.caughtUp === false) {
      dropped.notCaughtUp.push(p.symbol);
      continue;
    }
    out.push({ symbol: p.symbol, base: p.base, quote: p.quote, tick: Number(p.tick) });
  }
  return { pairs: out, dropped };
}

/** Preconditions, judged before any price is read: trades caught up and fetched after the window's end. */
export function preconditions(tapeRoot, toMs) {
  const lr = JSON.parse(readFileSync(join(tapeRoot, "state", "last-run.json"), "utf8"));
  const behind = lr.tasks?.binanceus?.symbolsBehind;
  const at = Date.parse(lr.at ?? "");
  const ok = behind === 0 && Number.isFinite(at) && at >= toMs;
  return { ok, symbolsBehind: behind ?? null, lastRun: lr.at ?? null };
}

const spreadBucket = (ticks) => (ticks <= 1 ? "1 tick" : ticks <= 4 ? "2-4 ticks" : "5+ ticks");

export function runQueueProv(tapeRoot, { fromMs, toMs, cfg = QUEUE, log = () => {} }) {
  const pre = preconditions(tapeRoot, toMs);
  if (!pre.ok) return { preconditions: pre };
  const uni = universe(tapeRoot, cfg);
  const syms = new Set(uni.pairs.map((p) => p.symbol));
  const q = readQuotes(tapeRoot, fromMs, toMs, { symbols: syms, cfg });
  // nothing after the window's end is known: the trades were fetched after it, not after any later poll
  q.intervals = q.intervals
    .map((iv) => ({
      ...iv,
      b: Math.min(iv.b, q.polls.filter((p) => p <= toMs).at(-1) ?? -Infinity),
    }))
    .filter((iv) => iv.b >= iv.a);
  log(`quotes: ${q.sessions.length} sessions, ${q.intervals.length} covered stretches`);
  const coverage = {
    sessions: q.sessions.length,
    sessionsWithoutEnd: q.sessions.filter((s) => !s.ended).length,
    failedPolls: q.sessions.reduce((a, s) => a + s.xs.length, 0),
    cappedSessions: q.sessions.filter((s) => s.cap != null).length,
    stretches: q.intervals.length,
    coveredMin: q.intervals.reduce((a, iv) => a + (iv.b - iv.a), 0) / MIN,
    spanMin: q.sessions.length ? (q.sessions.at(-1).end - q.sessions[0].start) / MIN : 0,
    firstPoll: q.polls.length ? new Date(q.polls[0]).toISOString() : null,
    lastPoll: q.polls.length ? new Date(q.polls.at(-1)).toISOString() : null,
  };
  const snaps = q.sessions.flatMap((s) => s.snaps).filter((t) => coverAt(q.intervals, t));

  const cells = new Map(); // `${variant}|${cancels}|${side}` -> accumulator
  const newCell = () => ({
    counts: {},
    fillTypes: {},
    ttf: [],
    ahead0Usd: [],
    edge0: [],
    midMove: Object.fromEntries(cfg.horizonsMin.map((h) => [h, []])),
    edge: Object.fromEntries(cfg.horizonsMin.map((h) => [h, []])),
    missing: Object.fromEntries(cfg.horizonsMin.map((h) => [h, 0])),
    taker: [],
    takerMissing: 0,
    takerThin: 0,
    maker: [],
    makerHow: {},
    bySpread: {},
    byPair: {},
  });
  const cellOf = (k) => {
    if (!cells.has(k)) cells.set(k, newCell());
    return cells.get(k);
  };
  const control = Object.fromEntries(
    cfg.sides.map((sd) => [sd, Object.fromEntries(cfg.horizonsMin.map((h) => [h, []]))]),
  );
  const calib = {};
  let pairsWithTrades = 0;
  let printsInWindow = 0;
  let placementsBase = 0;
  for (const pr of uni.pairs) {
    const s = q.series.get(pr.symbol);
    if (!s) continue;
    const tr = readTrades(tapeRoot, pr.symbol, fromMs, toMs + 60 * MIN);
    if (tr.length) pairsWithTrades++;
    printsInWindow += tr.filter((x) => coverAt(q.intervals, x.t)).length;
    calibratePair(q, s, tr, pr.tick, calib);
    const ctx = { q, s, tr, tick: pr.tick, restMs: cfg.restMs, latencyMs: cfg.latencyMs };
    for (const t0 of snaps) {
      const st = stateAt(q, s, t0);
      if (!st || !(st.bid > 0) || !(st.ask > st.bid)) continue;
      placementsBase++;
      const spreadTicks = Math.round((st.ask - st.bid) / pr.tick);
      const bucket = spreadBucket(spreadTicks);
      const m0 = midOf(st);
      for (const side of cfg.sides) {
        const sg = side === "buy" ? 1 : -1;
        for (const h of cfg.horizonsMin) {
          const m = midAt(q, s, t0 + h * MIN);
          if (m != null)
            control[side][h].push({
              value: (sg * (m - m0) * 1e4) / m0,
              cluster: Math.floor(t0 / 3_600_000),
            });
        }
        for (const variant of cfg.variants) {
          const touch = side === "buy" ? st.bid : st.ask;
          let price = touch;
          if (variant === "improve") {
            if (spreadTicks < 2) {
              for (const cancels of cfg.cancels) {
                const c = cellOf(`${variant}|${cancels}|${side}`);
                c.counts.noRoom = (c.counts.noRoom ?? 0) + 1;
              }
              continue;
            }
            price = touch + (side === "buy" ? pr.tick : -pr.tick);
          }
          const qty = cfg.notional / price;
          for (const cancels of cfg.cancels) {
            const c = cellOf(`${variant}|${cancels}|${side}`);
            const o = { side, t0, price, qty, cancels, variant };
            const r = simulate(o, ctx);
            const stKey = r.status === "expired" && r.filledQty > 0 ? "expiredPartial" : r.status;
            c.counts[stKey] = (c.counts[stKey] ?? 0) + 1;
            c.counts.placed = (c.counts.placed ?? 0) + 1;
            if (r.ahead0 != null && variant === "join") c.ahead0Usd.push(r.ahead0 * price);
            const sb = (c.bySpread[bucket] ??= {
              placed: 0,
              filled: 0,
              expired: 0,
              spreadBp: [],
              edge5: [],
              midMove5: [],
              maker: [],
            });
            sb.placed++;
            sb.spreadBp.push(((st.ask - st.bid) * 1e4) / m0);
            if (r.status === "expired") sb.expired++;
            if (r.status !== "filled") continue;
            sb.filled++;
            c.byPair[pr.symbol] = (c.byPair[pr.symbol] ?? 0) + 1;
            c.fillTypes[r.fillType] = (c.fillTypes[r.fillType] ?? 0) + 1;
            c.ttf.push((r.fillT - t0) / 1000);
            const out = outcomes(r, o, ctx, cfg);
            const cl = Math.floor(r.fillT / 3_600_000);
            if (out.edge0 != null) c.edge0.push({ value: out.edge0, cluster: cl });
            for (const h of cfg.horizonsMin) {
              if (out.midMove[h] != null) c.midMove[h].push({ value: out.midMove[h], cluster: cl });
              else c.missing[h]++;
              if (out.edge[h] != null) c.edge[h].push({ value: out.edge[h], cluster: cl });
            }
            if (out.edge[5] != null) sb.edge5.push({ value: out.edge[5], cluster: cl });
            if (out.midMove[5] != null) sb.midMove5.push({ value: out.midMove[5], cluster: cl });
            if (out.takerExit != null) {
              c.taker.push({ value: out.takerExit, cluster: cl });
              if (out.takerThin) c.takerThin++;
            } else c.takerMissing++;
            c.makerHow[out.maker.how] = (c.makerHow[out.maker.how] ?? 0) + 1;
            if (out.maker.net != null) {
              c.maker.push({ value: out.maker.net, cluster: cl });
              sb.maker.push({ value: out.maker.net, cluster: cl });
            }
          }
        }
      }
    }
  }
  log(`simulated ${placementsBase} placement instants`);

  const finish = (c) => {
    const n = c.counts;
    const filled = n.filled ?? 0;
    const expired = (n.expired ?? 0) + (n.expiredPartial ?? 0);
    const unknown = n.unknown ?? 0;
    return {
      counts: n,
      fillRateKnown: share(filled, filled + expired),
      unknownShare: share(unknown, filled + expired + unknown),
      fillTypes: c.fillTypes,
      timeToFillSec: { p50: pct(c.ttf, 0.5), p90: pct(c.ttf, 0.9) },
      queueAheadQuote: {
        p50: pct(c.ahead0Usd, 0.5),
        p90: pct(c.ahead0Usd, 0.9),
        zeroShare: share(c.ahead0Usd.filter((x) => x === 0).length, c.ahead0Usd.length),
      },
      halfSpreadAtFill: describe(c.edge0, cfg),
      midMove: Object.fromEntries(
        cfg.horizonsMin.map((h) => [h, { ...describe(c.midMove[h], cfg), missing: c.missing[h] }]),
      ),
      edgeVsMid: Object.fromEntries(cfg.horizonsMin.map((h) => [h, describe(c.edge[h], cfg)])),
      takerExit: {
        ...describe(c.taker, cfg),
        missing: c.takerMissing,
        topSizeBelowOrder: c.takerThin,
      },
      makerExit: { how: c.makerHow, net: describe(c.maker, cfg) },
      bySpread: Object.fromEntries(
        Object.entries(c.bySpread).map(([k, v]) => [
          k,
          {
            placed: v.placed,
            filled: v.filled,
            fillRateKnown: share(v.filled, v.filled + v.expired),
            spreadBpMedian: pct(v.spreadBp, 0.5),
            edgeVsMid5: describe(v.edge5, cfg),
            midMove5: describe(v.midMove5, cfg),
            makerExitNet: describe(v.maker, cfg),
          },
        ]),
      ),
      topPairs: Object.entries(c.byPair)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 10),
      pairsWithFills: Object.keys(c.byPair).length,
    };
  };
  const merge = (a, b) => {
    const m = newCell();
    for (const c of [a, b]) {
      for (const [k, v] of Object.entries(c.counts)) m.counts[k] = (m.counts[k] ?? 0) + v;
      for (const [k, v] of Object.entries(c.fillTypes)) m.fillTypes[k] = (m.fillTypes[k] ?? 0) + v;
      for (const [k, v] of Object.entries(c.makerHow)) m.makerHow[k] = (m.makerHow[k] ?? 0) + v;
      for (const [k, v] of Object.entries(c.byPair)) m.byPair[k] = (m.byPair[k] ?? 0) + v;
      m.ttf.push(...c.ttf);
      m.ahead0Usd.push(...c.ahead0Usd);
      m.edge0.push(...c.edge0);
      for (const h of cfg.horizonsMin) {
        m.midMove[h].push(...c.midMove[h]);
        m.edge[h].push(...c.edge[h]);
        m.missing[h] += c.missing[h];
      }
      m.taker.push(...c.taker);
      m.takerMissing += c.takerMissing;
      m.takerThin += c.takerThin;
      m.maker.push(...c.maker);
      for (const [k, v] of Object.entries(c.bySpread)) {
        const t = (m.bySpread[k] ??= {
          placed: 0,
          filled: 0,
          expired: 0,
          spreadBp: [],
          edge5: [],
          midMove5: [],
          maker: [],
        });
        t.placed += v.placed;
        t.filled += v.filled;
        t.expired += v.expired;
        t.spreadBp.push(...v.spreadBp);
        t.edge5.push(...v.edge5);
        t.midMove5.push(...v.midMove5);
        t.maker.push(...v.maker);
      }
    }
    return m;
  };
  const results = {};
  for (const variant of cfg.variants)
    for (const cancels of cfg.cancels) {
      const k = `${variant}|${cancels}`;
      const buy = cellOf(`${k}|buy`);
      const sell = cellOf(`${k}|sell`);
      results[k] = { buy: finish(buy), sell: finish(sell), both: finish(merge(buy, sell)) };
    }
  const silent = calib.silent ?? { n: 0, bidSizeChanged: 0, askSizeChanged: 0, relChange: [] };
  const calibration = {
    bid: calib.bid,
    ask: calib.ask,
    silentSizeChange: {
      snapshotsWithUnchangedPrices: silent.n,
      bidSizeChangedShare: share(silent.bidSizeChanged, silent.n),
      askSizeChangedShare: share(silent.askSizeChanged, silent.n),
      bidRelChangeMedian: pct(silent.relChange, 0.5),
    },
  };
  return {
    config: cfg,
    window: { from: new Date(fromMs).toISOString(), to: new Date(toMs).toISOString() },
    preconditions: pre,
    universe: {
      pairs: uni.pairs.length,
      pairsWithQuotes: uni.pairs.filter((p) => q.series.has(p.symbol)).length,
      pairsWithTrades,
      printsInCoverage: printsInWindow,
      dropped: uni.dropped,
    },
    coverage,
    placementInstants: { snapshots: snaps.length, pairInstants: placementsBase },
    calibration,
    control: Object.fromEntries(
      cfg.sides.map((sd) => [
        sd,
        Object.fromEntries(cfg.horizonsMin.map((h) => [h, describe(control[sd][h], cfg)])),
      ]),
    ),
    results,
  };
}

const gitInfo = (dir) => {
  try {
    const head = execFileSync("git", ["-C", dir, "rev-parse", "HEAD"], { encoding: "utf8" }).trim();
    const dirty =
      execFileSync("git", ["-C", dir, "status", "--porcelain"], { encoding: "utf8" }).trim() !== "";
    return { head, dirty };
  } catch {
    // a snapshot extracted with git archive names its commit in COMMIT
    const f = join(dir, "COMMIT");
    return {
      head: existsSync(f) ? readFileSync(f, "utf8").trim() : null,
      dirty: null,
      snapshot: true,
    };
  }
};

const isMain = process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1]);
if (isMain) {
  const arg = (k) => {
    const i = process.argv.indexOf(k);
    return i > 0 ? process.argv[i + 1] : undefined;
  };
  const tape = arg("--tape");
  const from = arg("--from");
  const to = arg("--to");
  if (!tape || !from || !to) {
    console.error("usage: queue.mjs --tape <dir> --from <ISO time> --to <ISO time> [--out file]");
    process.exit(2);
  }
  const res = runQueueProv(tape, {
    fromMs: Date.parse(from),
    toMs: Date.parse(to),
    log: (m) => console.error(m),
  });
  if (!res.preconditions.ok) {
    console.error(`preconditions not met: ${JSON.stringify(res.preconditions)}`);
    process.exit(3);
  }
  res.commits = { tape: gitInfo(tape), code: gitInfo(dirname(fileURLToPath(import.meta.url))) };
  const text = JSON.stringify(res, null, 1);
  if (arg("--out")) writeFileSync(arg("--out"), text + "\n");
  else console.log(text);
}
