#!/usr/bin/env node
// Data preparation for a possible FAITH study, P-OF2 (offshore open interest against US spot). Research data only:
// no account, no key, nothing here trades; any trading stays on US venues. 2026 is FAITH's holdout: every month here
// lies in 2022-01 to 2025-12 and the script refuses anything later. It reads metadata and timestamps only; it never
// computes anything from a price, a return or an open-interest value.
//   universe: pick the coins by the fixed rule in RULE from exchange metadata, write data/universe/pof2.json
//   list:     print "<BASE> <PERP_SYMBOL>" for the final list, in universe order
//   bytes:    print the bytes on disk of one coin's P-OF2 files (Coinbase candles5m and USD-M metrics)
//   stop:     record in data/universe/pof2-backfill.json that the size limit stopped the backfill at a coin
//   verify:   coverage from timestamps only, written to data/universe/pof2-coverage.json
// Usage: node recorder/pof2.mjs <command> [rootDir] [--run-id N] [args]
import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { makeFetcher, readJson, sleep, writeJson } from "./lib.mjs";
import { HOLDOUT, nextMonth } from "./backfill-binance-um.mjs";

export const FROM = "2022-01"; // first month of the study range
export const TO = "2026-01"; // exclusive: 2026 is FAITH's holdout
export const MAX_COINS = 15;
const ARCHIVE = "https://data.binance.vision/data/futures/um";
// The listing behind data.binance.vision's own index pages (the bucket it serves from).
const LISTING = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision";
const BUS = "https://api.binance.us";
const CB = "https://api.exchange.coinbase.com";

/** Throws unless [from, to) lies wholly before the 2026 holdout. */
export function refuseHoldout(from, to) {
  const ym = /^\d{4}-\d{2}$/;
  if (!ym.test(from) || !ym.test(to)) throw new Error(`months must be YYYY-MM, got ${from} and ${to}`);
  if (to > HOLDOUT || from >= HOLDOUT) throw new Error("2026 is FAITH's holdout and is not fetched here");
  if (from >= to) throw new Error(`empty range ${from} to ${to}`);
}
refuseHoldout(FROM, TO);

export const RULE = [
  "a. Base assets with a Binance.US spot pair against USD or USDT in status TRADING (api.binance.us /api/v3/exchangeInfo). Excluded: stablecoins and fiat-like or asset-pegged tokens, wrapped or staked duplicates, and BTC, ETH, SOL, DOGE and XRP (already used).",
  "b. Keep bases with a Coinbase Exchange <BASE>-USD product (api.exchange.coinbase.com /products) with status online and trading not disabled, whose five-minute candles exist on the first day of 2022-01 and on the first day of 2025-12 (existence only: the number of candles returned, never a price).",
  "c. Keep bases where data.binance.vision has the USD-M perp daily metrics zip for both 2022-01-01 and 2025-12-31 (HEAD, existence only). The perp symbol tried is <BASE>USDT, then 1000<BASE>USDT, 1000000<BASE>USDT and 1M<BASE>USDT; the first with both days is used.",
  "d. If more than 15 remain, keep the 15 whose USD-M monthly 5m klines archive starts earliest (first available month, from the archive listing), ties broken alphabetically by base. That same order (earliest first month, then base) is the universe order used if the backfill must stop for size.",
];

// Step a exclusions. A base is matched exactly (upper case).
export const STABLE = new Set([
  "USDT", "USDC", "DAI", "FDUSD", "TUSD", "PAX", "USDP", "BUSD", "GUSD", "PYUSD", "USD1", "USDE", "USDS", "USDD", "UST",
  "USTC", "FRAX", "LUSD", "SUSD", "RLUSD", "GHO", "CRVUSD", "FXUSD", "BFUSD", "XUSD", "AEUR", "EURC", "EURI", "EURT",
  "EUR", "GBP", "AUD", "CAD", "CHF", "JPY", "TRY", "BRL", "MXN", "ARS", "ZAR", "NGN", "RUB", "UAH", "IDR", "BIDR", "BVND",
  "USD",
]);
export const PEGGED = new Set(["PAXG", "XAUT"]); // gold-backed: priced as gold, not as a crypto asset
export const WRAPPED = new Set([
  "WBTC", "WETH", "STETH", "WSTETH", "CBETH", "RETH", "WBETH", "BETH", "WEETH", "EETH", "METH", "OSETH", "LSETH",
  "ANKRETH", "CBBTC", "BTCB", "TBTC", "RENBTC", "HBTC", "LBTC", "FBTC", "SOLVBTC", "WBNB", "BNSOL", "JITOSOL", "MSOL",
  "JUPSOL", "STSOL", "BBSOL", "WSOL", "SAVAX", "WAVAX", "STMATIC", "WMATIC", "WTRX", "WBETH",
]);
export const USED = new Set(["BTC", "ETH", "SOL", "DOGE", "XRP"]);

export const perpCandidates = (base) => [`${base}USDT`, `1000${base}USDT`, `1000000${base}USDT`, `1M${base}USDT`];

/** Step a from a Binance.US exchangeInfo answer. */
export function stepA(info) {
  const pairs = new Map();
  for (const s of info.symbols ?? []) {
    if (s.status !== "TRADING" || !["USD", "USDT"].includes(s.quoteAsset)) continue;
    const b = String(s.baseAsset).toUpperCase();
    if (!pairs.has(b)) pairs.set(b, []);
    pairs.get(b).push(s.symbol);
  }
  const candidates = [...pairs.keys()].sort();
  const kept = [];
  const dropped = [];
  for (const b of candidates) {
    const why = STABLE.has(b)
      ? "stablecoin or fiat-like"
      : PEGGED.has(b)
        ? "asset-pegged (gold-backed)"
        : WRAPPED.has(b)
          ? "wrapped or staked duplicate"
          : USED.has(b)
            ? "already used (BTC, ETH, SOL, DOGE, XRP)"
            : null;
    if (why) dropped.push({ base: b, why });
    else kept.push(b);
  }
  return { candidates: candidates.map((b) => ({ base: b, pairs: pairs.get(b).sort() })), kept, dropped };
}

const dayIso = (d) => `${d}T00:00:00Z`;
const nextDayIso = (d) => new Date(Date.parse(dayIso(d)) + 86_400_000).toISOString().replace(".000Z", "Z");

/** Step b. `products` is Coinbase's /products answer; `candleCount(id, day)` returns how many 5m candles that day has. */
export async function stepB(kept, products, candleCount) {
  const byId = new Map(products.map((p) => [p.id, p]));
  const out = [];
  const dropped = [];
  const probes = {};
  for (const b of kept) {
    const id = `${b}-USD`;
    const p = byId.get(id);
    if (!p) { dropped.push({ base: b, why: `no Coinbase product ${id}` }); continue; }
    if (p.status !== "online" || p.trading_disabled) {
      dropped.push({ base: b, why: `Coinbase ${id} status ${p.status}${p.trading_disabled ? ", trading disabled" : ""}` });
      continue;
    }
    const first = await candleCount(id, "2022-01-01");
    const last = await candleCount(id, "2025-12-01");
    probes[b] = { product: id, candles20220101: first, candles20251201: last };
    if (!(first > 0)) { dropped.push({ base: b, why: `no Coinbase 5m candles on 2022-01-01 (${first === null ? "probe failed" : "0 returned"})` }); continue; }
    if (!(last > 0)) { dropped.push({ base: b, why: `no Coinbase 5m candles on 2025-12-01 (${last === null ? "probe failed" : "0 returned"})` }); continue; }
    out.push(b);
  }
  return { kept: out, dropped, probes };
}

/** Step c. `exists(url)` returns true, false (404 or 403 from the archive) or null (could not tell). */
export async function stepC(kept, exists) {
  const out = [];
  const dropped = [];
  const probes = {};
  for (const b of kept) {
    const tried = [];
    let found = null;
    for (const sym of perpCandidates(b)) {
      const first = await exists(`${ARCHIVE}/daily/metrics/${sym}/${sym}-metrics-2022-01-01.zip`);
      const last = await exists(`${ARCHIVE}/daily/metrics/${sym}/${sym}-metrics-2025-12-31.zip`);
      tried.push({ sym, metrics20220101: first, metrics20251231: last });
      if (first === true && last === true) { found = sym; break; }
    }
    probes[b] = tried;
    if (!found) {
      const unsure = tried.some((t) => t.metrics20220101 === null || t.metrics20251231 === null);
      const partial = tried.filter((t) => t.metrics20220101 || t.metrics20251231).map((t) => `${t.sym} has ${t.metrics20220101 ? "2022-01-01" : "2025-12-31"} only`);
      dropped.push({ base: b, why: unsure ? "archive probe failed (could not tell)" : partial.length ? partial.join("; ") : "no USD-M daily metrics for 2022-01-01 or 2025-12-31 under any tried symbol" });
      continue;
    }
    out.push({ base: b, perp: found, renamed: found !== `${b}USDT` });
  }
  return { kept: out, dropped, probes };
}

/** First YYYY-MM among listed keys "<prefix><SYM>-5m-YYYY-MM.zip". */
export function firstMonthFromKeys(keys, sym) {
  const re = new RegExp(`/${sym}-5m-(\\d{4}-\\d{2})\\.zip$`);
  const months = keys.map((k) => re.exec(k)?.[1]).filter(Boolean).sort();
  return months[0] ?? null;
}

/** Step d: order by first month then base; keep MAX_COINS. `firstMonth(sym)` returns {month, method}. */
export async function stepD(kept, firstMonth) {
  const rows = [];
  for (const k of kept) rows.push({ ...k, ...(await firstMonth(k.perp)) });
  rows.sort((x, y) => (x.firstKlinesMonth ?? "9999") < (y.firstKlinesMonth ?? "9999") ? -1 : (x.firstKlinesMonth ?? "9999") > (y.firstKlinesMonth ?? "9999") ? 1 : x.base < y.base ? -1 : x.base > y.base ? 1 : 0);
  const applied = rows.length > MAX_COINS;
  return { applied, ordered: rows, kept: rows.slice(0, MAX_COINS), cut: rows.slice(MAX_COINS).map((r) => ({ base: r.base, why: `rank ${rows.indexOf(r) + 1} of ${rows.length} by first klines month (${r.firstKlinesMonth})` })) };
}

async function universe(root, runId) {
  const config = readJson(join(root, "recorder", "config.json"), {});
  const f = makeFetcher({ spacingMs: config.spacingMs ?? {}, timeoutMs: 30_000 });
  const info = (await f.getJson(`${BUS}/api/v3/exchangeInfo`)).json;
  const a = stepA(info);
  const products = (await f.getJson(`${CB}/products`)).json;
  const candleCount = async (id, day) => {
    const q = new URLSearchParams({ granularity: "300", start: dayIso(day), end: nextDayIso(day) });
    try {
      const { json } = await f.getJson(`${CB}/products/${id}/candles?${q}`);
      const lo = Date.parse(dayIso(day)) / 1000;
      return json.filter((r) => r[0] >= lo && r[0] < lo + 86_400).length; // timestamps only
    } catch (e) {
      return e.status === 404 ? 0 : null;
    }
  };
  const b = await stepB(a.kept, products, candleCount);
  const raw = async (url, method = "HEAD") => {
    for (let attempt = 0; ; attempt += 1) {
      const ctrl = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), 30_000);
      try {
        const res = await fetch(url, { method, headers: { "user-agent": "faith-tape-recorder/1" }, signal: ctrl.signal });
        if (method === "GET") return { status: res.status, text: await res.text() };
        return { status: res.status };
      } catch (e) {
        if (attempt >= 3) return { status: null, error: e.message };
        await sleep(1000 * 2 ** attempt);
      } finally {
        clearTimeout(timer);
      }
    }
  };
  const exists = async (url) => {
    const r = await raw(url);
    if (r.status === 200) return true;
    if (r.status === 404 || r.status === 403) return false; // the archive answers 404 (or S3's 403) for a missing key
    return null;
  };
  const c = await stepC(b.kept, exists);
  const firstMonth = async (sym) => {
    const prefix = `data/futures/um/monthly/klines/${sym}/5m/`;
    const keys = [];
    let marker = "";
    let ok = true;
    for (let page = 0; page < 10; page += 1) {
      const q = new URLSearchParams({ delimiter: "/", prefix, ...(marker ? { marker } : {}) });
      const r = await raw(`${LISTING}?${q}`, "GET");
      if (r.status !== 200) { ok = false; break; }
      const got = [...r.text.matchAll(/<Key>([^<]+)<\/Key>/g)].map((m) => m[1]);
      keys.push(...got);
      if (!/<IsTruncated>true<\/IsTruncated>/.test(r.text) || !got.length) break;
      marker = got[got.length - 1];
    }
    const listed = ok ? firstMonthFromKeys(keys, sym) : null;
    if (listed) return { firstKlinesMonth: listed, method: "listing" };
    // Fallback when the listing cannot be read: HEAD each month from 2019-09 (before the archive starts) to 2022-01.
    for (let ym = "2019-09"; ym <= FROM; ym = nextMonth(ym)) {
      if (await exists(`${ARCHIVE}/monthly/klines/${sym}/5m/${sym}-5m-${ym}.zip`)) return { firstKlinesMonth: ym, method: "head-scan" };
    }
    return { firstKlinesMonth: null, method: ok ? "listing (none found), head-scan (none found)" : "listing failed, head-scan (none found)" };
  };
  const d = await stepD(c.kept, firstMonth);
  const doc = {
    study: "P-OF2",
    note: "Open-interest research data for a possible FAITH study; research only; trading stays on US venues; 2026 is FAITH's holdout. Metadata and existence probes only: no price, return or open-interest value was read.",
    rule: RULE,
    range: { from: FROM, to: "2025-12" },
    runId: runId ?? null,
    at: new Date().toISOString(),
    steps: {
      a: { source: `${BUS}/api/v3/exchangeInfo`, candidates: a.candidates, kept: a.kept, dropped: a.dropped },
      b: { source: `${CB}/products and /products/<id>/candles`, candidates: a.kept, kept: b.kept, dropped: b.dropped, probes: b.probes },
      c: { source: `${ARCHIVE}/daily/metrics/`, candidates: b.kept, kept: c.kept.map((k) => k.base), dropped: c.dropped, probes: c.probes, symbolDiffers: c.kept.filter((k) => k.renamed).map((k) => ({ base: k.base, perp: k.perp })) },
      d: { source: `${LISTING}?prefix=data/futures/um/monthly/klines/<SYM>/5m/`, candidates: d.ordered.map((r) => ({ base: r.base, perp: r.perp, firstKlinesMonth: r.firstKlinesMonth, method: r.method })), applied: d.applied, kept: d.kept.map((r) => r.base), cut: d.cut },
    },
    final: d.kept.map((r) => ({ base: r.base, coinbase: `${r.base}-USD`, perp: r.perp, firstKlinesMonth: r.firstKlinesMonth })),
    calls: f.calls,
  };
  writeJson(join(root, "data", "universe", "pof2.json"), doc);
  console.log(JSON.stringify({ a: a.kept.length, b: b.kept.length, c: c.kept.length, final: doc.final.map((x) => `${x.base}:${x.perp}`) }));
}

const files = (dir) => (existsSync(dir) ? readdirSync(dir).filter((n) => /^\d{4}-\d{2}\.ndjson\.gz$/.test(n)).sort() : []);
const monthsIn = () => {
  const out = [];
  for (let ym = FROM; ym < TO; ym = nextMonth(ym)) out.push(ym);
  return out;
};
const dirBytes = (dir) => (existsSync(dir) ? readdirSync(dir).reduce((s, n) => s + statSync(join(dir, n)).size, 0) : 0);
export const coinDirs = (root, base, perp) => [join(root, "data", "coinbase", "candles5m", base), join(root, "data", "binance-um", "metrics", perp)];

/** First field of every row of a gzip NDJSON file: the timestamp. Nothing else is read. */
export function timestamps(path) {
  return gunzipSync(readFileSync(path)).toString().split("\n").filter((l) => l).map((l) => JSON.parse(l)[0]);
}

/** Coverage for one coin from timestamps only. Months are those of [FROM, TO). */
export function coverage(root, base, perp) {
  const [cbDir, mDir] = coinDirs(root, base, perp);
  const all = monthsIn();
  const lo = Date.UTC(2022, 0, 1);
  const hi = Date.UTC(2026, 0, 1);
  const slots = (hi - lo) / 300_000;
  const read = (dir) => {
    const present = files(dir).map((n) => n.slice(0, 7)).filter((m) => m >= FROM && m < TO);
    const ts = new Set();
    let offGrid = 0;
    let outside = 0;
    for (const m of present) for (const t of timestamps(join(dir, `${m}.ndjson.gz`))) {
      if (t < lo || t >= hi) { outside += 1; continue; }
      if (t % 300_000 !== 0) offGrid += 1;
      ts.add(t);
    }
    return { present, missingMonths: all.filter((m) => !present.includes(m)), ts, offGrid, outside, bytes: dirBytes(dir) };
  };
  const cb = read(cbDir);
  const me = read(mDir);
  const onGrid = (ts) => [...ts].filter((t) => t % 300_000 === 0).length;
  // Longest stretch with no row (from the range start, between rows, to the range end) and UTC days with no row.
  const gaps = (ts) => {
    const s = [lo - 300_000, ...[...ts].sort((x, y) => x - y), hi];
    let best = { minutes: 0, from: null };
    for (let i = 1; i < s.length; i += 1) {
      const m = (s[i] - s[i - 1]) / 60_000 - 5;
      if (m > best.minutes) best = { minutes: m, from: new Date(s[i - 1] + 300_000).toISOString().slice(0, 16) + "Z" };
    }
    const days = new Set([...ts].map((t) => Math.floor(t / 86_400_000)));
    return { longestGapMinutes: best.minutes, longestGapFrom: best.from, daysWithoutRow: (hi - lo) / 86_400_000 - days.size };
  };
  let hours = 0;
  let both = 0;
  for (let t = lo + 3_600_000; t < hi; t += 3_600_000) {
    hours += 1;
    if (me.ts.has(t) && me.ts.has(t - 3_600_000)) both += 1;
  }
  const missingJson = join(mDir, "missing.json");
  const r4 = (x) => Math.round(x * 1e4) / 1e4;
  return {
    base,
    perp,
    coinbase: {
      months: cb.present.length,
      missingMonths: cb.missingMonths,
      firstMonth: cb.present[0] ?? null,
      lastMonth: cb.present.at(-1) ?? null,
      candles: cb.ts.size,
      slots,
      slotShare: r4(onGrid(cb.ts) / slots),
      offGrid: cb.offGrid,
      ...gaps(cb.ts),
      bytes: cb.bytes,
    },
    metrics: {
      months: me.present.length,
      missingMonths: me.missingMonths,
      archiveMissing: existsSync(missingJson) ? JSON.parse(readFileSync(missingJson, "utf8")).missing : [],
      rows: me.ts.size,
      slotShare: r4(onGrid(me.ts) / slots),
      offGrid: me.offGrid,
      ...gaps(me.ts),
      hours,
      hoursWithTandTminus1h: both,
      hourPairShare: r4(both / hours),
      bytes: me.bytes + (existsSync(missingJson) ? statSync(missingJson).size : 0),
    },
  };
}

function verify(root, runId) {
  const u = readJson(join(root, "data", "universe", "pof2.json"), null);
  if (!u) throw new Error("no data/universe/pof2.json");
  const bf = readJson(join(root, "data", "universe", "pof2-backfill.json"), {});
  const coins = u.final.filter((c) => !bf.stoppedAt || u.final.indexOf(c) < u.final.findIndex((x) => x.base === bf.stoppedAt));
  const rows = coins.map((c) => coverage(root, c.base, c.perp));
  const total = rows.reduce((s, r) => s + r.coinbase.bytes + r.metrics.bytes, 0);
  writeJson(join(root, "data", "universe", "pof2-coverage.json"), {
    study: "P-OF2",
    note: "From timestamps only (the first field of each row). slotShare: five-minute slots of 2022-01-01 to 2025-12-31 with a row (Coinbase writes no candle for five minutes without a trade, so a thin coin's empty slots are mostly quiet minutes, not outages). longestGapMinutes/longestGapFrom: the longest stretch with no row; daysWithoutRow: UTC days with none. hourPairShare: whole hours T (01:00 2022-01-01 to 23:00 2025-12-31) with a metrics row at both T and T-1h. offGrid: rows not on a five-minute boundary.",
    runId: runId ?? null,
    at: new Date().toISOString(),
    stoppedAt: bf.stoppedAt ?? null,
    totalBytes: total,
    coins: rows,
  });
  for (const r of rows) console.log(`${r.base} ${r.perp} cb ${r.coinbase.months}m ${r.coinbase.slotShare} ${(r.coinbase.bytes / 1e6).toFixed(1)}MB | oi ${r.metrics.months}m ${r.metrics.hourPairShare} ${(r.metrics.bytes / 1e6).toFixed(1)}MB`);
  console.log(`total ${(total / 1e6).toFixed(1)} MB`);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const [cmd, ...args] = process.argv.slice(2);
  const opt = (k, d) => (args.includes(k) ? args[args.indexOf(k) + 1] : d);
  const pos = args.filter((a, i) => !a.startsWith("--") && !args[i - 1]?.startsWith("--"));
  const root = resolve(pos[0] ?? ".");
  const runId = opt("--run-id", null);
  if (cmd === "universe") await universe(root, runId);
  else if (cmd === "list") {
    for (const c of readJson(join(root, "data", "universe", "pof2.json"), { final: [] }).final) console.log(`${c.base} ${c.perp}`);
  } else if (cmd === "bytes") {
    console.log(coinDirs(root, pos[1], pos[2]).reduce((s, d) => s + dirBytes(d), 0));
  } else if (cmd === "stop") {
    const p = join(root, "data", "universe", "pof2-backfill.json");
    writeJson(p, { ...readJson(p, {}), stoppedAt: pos[1], bytesBefore: Number(pos[2]), bytesWithIt: Number(pos[3]), limitBytes: Number(pos[4]), runId, note: "the coin at stoppedAt and every coin after it in universe order were not added, because adding it would pass the size limit" });
  } else if (cmd === "verify") verify(root, runId);
  else throw new Error("usage: pof2.mjs universe|list|bytes|stop|verify [root]");
}
