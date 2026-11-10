// Kraken public spot data: USD pairs and trades. No key. Kraken's `since` wording is ambiguous (timestamp or
// id), so the recorder keeps the last trade id it wrote and drops anything at or below it: overlap is safe,
// a skipped trade is not.
const BASE = "https://api.kraken.com";

// Kraken's legacy asset codes for the few bases where they differ from the common ticker.
const ALIAS = { XBT: "BTC", XDG: "DOGE" };

export async function usdPairs(f) {
  const { json } = await f.getJson(`${BASE}/0/public/AssetPairs`);
  if (json.error?.length) throw new Error(`kraken AssetPairs: ${json.error.join(",")}`);
  const out = {};
  for (const [key, p] of Object.entries(json.result)) {
    if (!p.wsname || p.status && p.status !== "online") continue;
    const [b, quote] = p.wsname.split("/");
    if (quote !== "USD") continue;
    const base = ALIAS[b] ?? b;
    out[base] = { key, altname: p.altname };
  }
  return out;
}

/**
 * Trades after `state` ({since, lastId}), oldest first. Pages while each page is full.
 * Returns trades {id, t, p, q, side} and the `last` value Kraken gives for the next poll.
 */
export async function tradesSince(f, pair, state, { maxPages = 10, deadline, startSinceNs } = {}) {
  let since = state?.since ?? startSinceNs;
  const lastId = state?.lastId ?? -1;
  const out = [];
  let pages = 0;
  let full = false;
  while (pages < maxPages && !deadline?.passed()) {
    const q = new URLSearchParams({ pair: pair.altname, count: "1000" });
    if (since != null) q.set("since", String(since));
    const { json } = await f.getJson(`${BASE}/0/public/Trades?${q}`);
    if (json.error?.length) throw new Error(`kraken Trades ${pair.altname}: ${json.error.join(",")}`);
    pages += 1;
    const key = Object.keys(json.result).find((k) => k !== "last");
    const rows = json.result[key] ?? [];
    for (const r of rows) {
      const id = Number(r[6]);
      if (!(id > lastId)) continue;
      out.push({ id, t: Math.round(Number(r[2]) * 1000), p: Number(r[0]), q: Number(r[1]), side: r[3] === "b" ? "buy" : "sell" });
    }
    since = json.result.last;
    full = rows.length >= 1000;
    if (!full) break;
  }
  out.sort((a, b) => a.id - b.id);
  return { trades: out.filter((t, i) => i === 0 || t.id !== out[i - 1].id), last: since, caughtUp: !full, pages };
}
