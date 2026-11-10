// Binance.US public spot data: pair list, aggregated trades by id (a complete tape) and book snapshots.
// No API key. aggTrades ids are sequential per symbol, so paging by fromId never skips a trade.
const BASE = "https://api.binance.us";

export async function listPairs(f, quotes) {
  const { json } = await f.getJson(`${BASE}/api/v3/exchangeInfo`);
  return json.symbols
    .filter((s) => s.status === "TRADING" && quotes.includes(s.quoteAsset))
    .map((s) => {
      const pf = s.filters.find((x) => x.filterType === "PRICE_FILTER");
      const ls = s.filters.find((x) => x.filterType === "LOT_SIZE");
      return { symbol: s.symbol, base: s.baseAsset, quote: s.quoteAsset, tick: pf?.tickSize, step: ls?.stepSize };
    })
    .sort((a, b) => (a.symbol < b.symbol ? -1 : 1));
}

const row = (t) => [t.a, t.T, Number(t.p), Number(t.q), t.m ? 1 : 0];

async function agg(f, symbol, params) {
  const q = new URLSearchParams({ symbol, ...params });
  const { json, headers } = await f.getJson(`${BASE}/api/v3/aggTrades?${q}`);
  const used = Number(headers?.get?.("x-mbx-used-weight-1m") ?? 0);
  return { trades: json, used };
}

/**
 * Smallest aggregate-trade id whose time is at or after startMs, or null when no trade is that late.
 * One time-window call first; a binary search on ids when the first hour after startMs is empty.
 */
export async function firstIdAtOrAfter(f, symbol, startMs) {
  const w = await agg(f, symbol, { startTime: String(startMs), endTime: String(startMs + 3_599_999), limit: "1" });
  if (w.trades.length) return w.trades[0].a;
  const latest = await agg(f, symbol, { limit: "1" });
  if (!latest.trades.length) return null;
  const top = latest.trades[latest.trades.length - 1];
  if (top.T < startMs) return null;
  let lo = 0;
  let hi = top.a;
  while (lo < hi) {
    const mid = Math.floor((lo + hi) / 2);
    const r = await agg(f, symbol, { fromId: String(mid), limit: "1" });
    const t = r.trades.length ? r.trades[0].T : Infinity;
    if (t >= startMs) hi = mid;
    else lo = mid + 1;
  }
  return lo;
}

/**
 * Pull trades after the cursor (the last id written) until caught up, out of pages or out of time.
 * Returns rows [id, ms, price, qty, buyerIsMaker] and the new cursor.
 */
export async function pullTrades(f, symbol, cursor, { deadline, maxPages = 50, onWeight } = {}) {
  const rows = [];
  let next = cursor + 1;
  let pages = 0;
  let caughtUp = false;
  while (pages < maxPages && !deadline?.passed()) {
    const { trades, used } = await agg(f, symbol, { fromId: String(next), limit: "1000" });
    pages += 1;
    if (onWeight) await onWeight(used);
    for (const t of trades) {
      if (t.a < next) continue; // defensive: never write an id twice
      rows.push(row(t));
      next = t.a + 1;
    }
    if (trades.length < 1000) {
      caughtUp = true;
      break;
    }
  }
  return { rows, cursor: next - 1, caughtUp, pages };
}

/** Top-of-book snapshot, `levels` deep on each side. Row: [ms, lastUpdateId, bids, asks]. */
export async function depth(f, symbol, levels, nowMs) {
  const { json } = await f.getJson(`${BASE}/api/v3/depth?symbol=${symbol}&limit=${levels}`);
  const side = (xs) => xs.slice(0, levels).map(([p, q]) => [Number(p), Number(q)]);
  return [nowMs, json.lastUpdateId, side(json.bids), side(json.asks)];
}

/** 24-hour stats for every symbol in one call: [symbol, count, quoteVolume, bidPrice, askPrice]. */
export async function stats24h(f) {
  const { json } = await f.getJson(`${BASE}/api/v3/ticker/24hr`);
  return json.map((s) => [s.symbol, s.count, Number(s.quoteVolume), Number(s.bidPrice ?? 0), Number(s.askPrice ?? 0)]);
}
