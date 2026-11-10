// Kalshi perpetuals: public market list with bid, ask and funding fields. No key. Read only.
const BASE = "https://external-api.kalshi.com/trade-api/v2/margin";

/** All margin markets, following the cursor if the endpoint pages. Returns {markets, pages}. */
export async function marginMarkets(f, { maxPages = 10 } = {}) {
  const markets = [];
  let cursor = "";
  let pages = 0;
  while (pages < maxPages) {
    const { json } = await f.getJson(`${BASE}/markets${cursor ? `?cursor=${encodeURIComponent(cursor)}` : ""}`);
    pages += 1;
    markets.push(...(json.markets ?? []));
    cursor = json.cursor ?? "";
    if (!cursor) break;
  }
  return { markets, pages };
}
