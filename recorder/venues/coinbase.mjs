// Coinbase Exchange public market data: USD products, recent trades and one-minute candles. No key.
// Trades come newest first; the `after` cursor pages to older trades. To catch up from a stored trade id
// the recorder pages backwards from the newest trade until it reaches that id, which needs no assumption
// about how the `before` cursor orders its page.
const BASE = "https://api.exchange.coinbase.com";

export async function usdProducts(f) {
  const { json } = await f.getJson(`${BASE}/products`);
  const out = {};
  for (const p of json) {
    if (p.quote_currency !== "USD" || p.status !== "online" || p.trading_disabled) continue;
    out[p.base_currency] = p.id;
  }
  return out;
}

const toTrade = (t) => ({ id: Number(t.trade_id), t: Date.parse(t.time), p: Number(t.price), q: Number(t.size), side: t.side });

/**
 * Trades newer than `cursor` (a trade id), oldest first. When the cursor is unknown only the newest page
 * is taken. `gap` is true when the page budget ran out before reaching the cursor, so the caller can
 * record exactly which ids are missing instead of pretending the tape is whole.
 */
export async function tradesSince(f, product, cursor, { maxPages = 40, deadline } = {}) {
  const collected = [];
  let after = null;
  let pages = 0;
  let reached = cursor == null;
  while (pages < maxPages && !deadline?.passed()) {
    const q = new URLSearchParams({ limit: "1000" });
    if (after) q.set("after", after);
    const { json, headers } = await f.getJson(`${BASE}/products/${product}/trades?${q}`);
    pages += 1;
    if (!json.length) {
      reached = true;
      break;
    }
    for (const t of json) {
      const tr = toTrade(t);
      if (cursor != null && tr.id <= cursor) {
        reached = true;
        continue;
      }
      collected.push(tr);
    }
    if (reached || cursor == null) break;
    after = headers?.get?.("cb-after") ?? String(Math.min(...json.map((t) => Number(t.trade_id))));
  }
  collected.sort((a, b) => a.id - b.id);
  const dedup = collected.filter((t, i) => i === 0 || t.id !== collected[i - 1].id);
  const oldest = dedup.length ? dedup[0].id : null;
  return { trades: dedup, gap: !reached, gapFrom: cursor, gapTo: oldest, pages };
}

/** One-minute candles in [startMs, endMs), oldest first. Row: [ms, open, high, low, close, volume]. */
export async function candles1m(f, product, startMs, endMs) {
  const q = new URLSearchParams({ granularity: "60", start: new Date(startMs).toISOString(), end: new Date(endMs).toISOString() });
  const { json } = await f.getJson(`${BASE}/products/${product}/candles?${q}`);
  return json
    .map(([t, low, high, open, close, vol]) => [t * 1000, open, high, low, close, vol])
    .filter((r) => r[0] >= startMs && r[0] < endMs)
    .sort((a, b) => a[0] - b[0]);
}
