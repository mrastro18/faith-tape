// Copied unchanged from FAITH (github.com/mrastro18/faith) scripts/loop/tape.mjs at commit b801165a0eec3203a7471eab7f60ac11d3a0bc44
// (branch claude/queue-sim). Only these two header lines are added. Do not edit here: change FAITH and copy again.
// Readers for the faith-tape data repository (mrastro18/faith-tape). Pure file reads: no network.
// Layout and row formats are documented in that repository's README.
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";

const days = (dir) =>
  existsSync(dir)
    ? readdirSync(dir)
        .filter((d) => /^\d{4}-\d{2}-\d{2}/.test(d))
        .sort()
    : [];
const nd = (path) =>
  existsSync(path)
    ? readFileSync(path, "utf8")
        .split("\n")
        .filter(Boolean)
        .map((l) => JSON.parse(l))
    : [];
const inDays = (d, fromMs, toMs) => {
  const day = Date.parse(d.slice(0, 10) + "T00:00:00Z");
  return day + 86_400_000 > fromMs && day < toMs;
};

/**
 * Binance.US aggregate trades for one symbol in [fromMs, toMs): {id, t, p, q, buyerMaker}, sorted by time
 * and then by id (the scorer's fill rule walks them in time order).
 */
export function trades(root, symbol, fromMs = 0, toMs = Infinity) {
  const dir = join(root, "data", "binanceus", "trades");
  const out = [];
  for (const d of days(dir)) {
    if (!inDays(d, fromMs, toMs)) continue;
    for (const r of nd(join(dir, d, `${symbol}.ndjson`)))
      if (r[1] >= fromMs && r[1] < toMs)
        out.push({ id: r[0], t: r[1], p: r[2], q: r[3], buyerMaker: r[4] === 1 });
  }
  out.sort((a, b) => a.t - b.t || a.id - b.id);
  return out;
}

/**
 * Reference bars for one base: one-second bars from `venue` ("coinbase" or "kraken") and, before those
 * begin, Coinbase one-minute candles. Each bar is {s: start ms, e: end ms, c: close, v: vwap, n: trades}.
 * A minute candle's value is known only at its end, which is what `priceBefore` uses.
 */
export function referenceBars(root, venue, base, fromMs = 0, toMs = Infinity) {
  const out = [];
  const secDir = join(root, "data", venue, "bars1s");
  for (const d of days(secDir)) {
    if (!inDays(d, fromMs, toMs)) continue;
    for (const r of nd(join(secDir, d, `${base}.ndjson`))) {
      const s = r[0] * 1000;
      if (s >= fromMs && s < toMs) out.push({ s, e: s + 1000, c: r[4], v: r[7], n: r[6] });
    }
  }
  if (venue === "coinbase") {
    const first = out.reduce((m, b) => (b.s < m ? b.s : m), Infinity); // no spread: days of bars
    const minDir = join(root, "data", "coinbase", "candles1m");
    for (const d of days(minDir)) {
      if (!inDays(d, fromMs, toMs)) continue;
      for (const r of nd(join(minDir, d, `${base}.ndjson`)))
        if (r[0] >= fromMs && r[0] < toMs && r[0] + 60_000 <= first && r[5] > 0)
          out.push({ s: r[0], e: r[0] + 60_000, c: r[4], v: r[4], n: null });
    }
  }
  out.sort((a, b) => a.e - b.e);
  return out;
}

/**
 * A point-in-time price lookup over bars sorted by end time. `before(ms)` returns the close of the last bar
 * that ended strictly before `ms`, if it ended no more than `maxAgeMs` earlier; otherwise null. A bar that
 * ends exactly at `ms` is not yet known at `ms`. This is the join the look-ahead test covers, and it uses the
 * same strict cut.
 */
export function pricer(bars, maxAgeMs = 60_000) {
  const ends = bars.map((b) => b.e);
  return {
    before(ms) {
      let lo = 0;
      let hi = ends.length - 1;
      let at = -1;
      while (lo <= hi) {
        const mid = (lo + hi) >> 1;
        if (ends[mid] < ms) {
          at = mid;
          lo = mid + 1;
        } else hi = mid - 1;
      }
      if (at < 0 || ms - ends[at] > maxAgeMs) return null;
      return bars[at].c;
    },
    bars,
  };
}

/** Book snapshots for one symbol: {t, bid, ask, bids, asks}. */
export function depth(root, symbol, fromMs = 0, toMs = Infinity) {
  const dir = join(root, "data", "binanceus", "depth");
  const out = [];
  if (!existsSync(dir)) return out;
  for (const f of readdirSync(dir).sort()) {
    if (!inDays(f, fromMs, toMs)) continue;
    for (const r of nd(join(dir, f))) {
      if (r[0] !== symbol || r[1] < fromMs || r[1] >= toMs) continue;
      const bids = r[3];
      const asks = r[4];
      out.push({ t: r[1], bid: bids[0]?.[0] ?? null, ask: asks[0]?.[0] ?? null, bids, asks });
    }
  }
  return out;
}

/**
 * Pairs listed on the latest pair list known strictly before `ms`. A list dated day D was written during D,
 * so it counts as known from the start of D + 1. With no list known before `ms` there is no universe: []
 * (never a later list, which would put pairs listed in the future into the past).
 */
export function pairs(root, ms = Infinity) {
  const dir = join(root, "data", "binanceus", "pairs");
  const files = existsSync(dir)
    ? readdirSync(dir)
        .filter((f) => /^\d{4}-\d{2}-\d{2}.*\.json$/.test(f))
        .sort()
    : [];
  const pick = files
    .filter((f) => Date.parse(f.slice(0, 10) + "T00:00:00Z") + 86_400_000 < ms)
    .at(-1);
  return pick ? JSON.parse(readFileSync(join(dir, pick), "utf8")) : [];
}
