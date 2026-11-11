#!/usr/bin/env node
// One-off history: Binance USD-M perpetual futures from Binance's public archive (data.binance.vision), 2021-01 to
// 2025-12, as signals for US-spot research. Research data only: no account, no key, and nothing here trades; any
// trading stays on US venues. It stops at 2026-01-01 on purpose: 2026 is FAITH's holdout and is not fetched here.
// Every archive is checked against its .CHECKSUM (sha256) before it is read. A file already on disk is skipped, so
// a cut-off run resumes. A month (or year) with a failed fetch is not written; one the archive does not have (404)
// is listed in data/binance-um/<kind>/<SYM>/missing.json.
//   klines:  data/binance-um/klines5m/<SYM>/<YYYY-MM>.ndjson.gz  [openMs, o, h, l, c, volume, quoteVolume, trades, takerBuyVolume]
//   funding: data/binance-um/funding/<SYM>/<YYYY>.ndjson.gz      [fundingTimeMs, rate]
//   metrics: data/binance-um/metrics/<SYM>/<YYYY-MM>.ndjson.gz   [ms, sumOpenInterest, sumOpenInterestValue, ...METRIC_RATIOS]
//            (--metrics-cols oi: [ms, sumOpenInterest, sumOpenInterestValue]; sumoi: [ms, sumOpenInterest] only)
// Usage: node recorder/backfill-binance-um.mjs [rootDir] [--syms BTCUSDT,ETHUSDT] [--kinds klines,funding,metrics]
//          [--from 2021-01] [--to 2026-01] [--metrics-from 2021-01] [--metrics-cols full|oi|sumoi]
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { crc32, gzipSync, inflateRawSync } from "node:zlib";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { sha256, sleep } from "./lib.mjs";

const BASE = "https://data.binance.vision/data/futures/um";
export const HOLDOUT = "2026-01"; // first month never fetched: 2026 is sealed
export const KINDS = ["klines", "funding", "metrics"];
export const METRIC_COLS = ["full", "oi", "sumoi"];
export const METRIC_RATIOS = ["topTraderAccountLS", "topTraderPositionLS", "accountLS", "takerBuySellVolRatio"];
const METRIC_SRC = ["count_toptrader_long_short_ratio", "sum_toptrader_long_short_ratio", "count_long_short_ratio", "sum_taker_long_short_vol_ratio"];

const monthStart = (ym) => Date.UTC(Number(ym.slice(0, 4)), Number(ym.slice(5, 7)) - 1, 1);
export const nextMonth = (ym) => {
  const d = new Date(monthStart(ym));
  d.setUTCMonth(d.getUTCMonth() + 1);
  return d.toISOString().slice(0, 7);
};
const daysOf = (ym) => {
  const out = [];
  for (let t = monthStart(ym); t < monthStart(nextMonth(ym)); t += 86_400_000) out.push(new Date(t).toISOString().slice(0, 10));
  return out;
};

/** Throws unless [from, to) lies wholly before the 2026 holdout. `to` is exclusive, as in backfill.mjs. */
export function checkRange(from, to) {
  const ym = /^\d{4}-\d{2}$/;
  if (!ym.test(from) || !ym.test(to)) throw new Error(`months must be YYYY-MM, got ${from} and ${to}`);
  if (to > HOLDOUT || from >= HOLDOUT) throw new Error("2026 is FAITH's holdout and is not fetched here");
  if (from >= to) throw new Error(`empty range ${from} to ${to}`);
}

/**
 * Read the one file of a zip archive: find the end-of-central-directory record, take the first central entry's
 * method, sizes and local header offset (the local header's sizes are zero when a data descriptor follows), then
 * inflate the raw deflate stream and check its CRC-32. Stored (method 0) entries are returned as they are.
 */
export function unzipSingle(buf) {
  let eocd = -1;
  for (let i = buf.length - 22; i >= Math.max(0, buf.length - 22 - 65535); i -= 1) {
    if (buf.readUInt32LE(i) === 0x06054b50) {
      eocd = i;
      break;
    }
  }
  if (eocd < 0) throw new Error("zip: no end of central directory");
  const entries = buf.readUInt16LE(eocd + 10);
  const cd = buf.readUInt32LE(eocd + 16);
  if (entries < 1) throw new Error("zip: no entries");
  if (cd === 0xffffffff || buf.readUInt32LE(cd) !== 0x02014b50) throw new Error("zip: bad central directory");
  const method = buf.readUInt16LE(cd + 10);
  const crc = buf.readUInt32LE(cd + 16);
  const compSize = buf.readUInt32LE(cd + 20);
  const size = buf.readUInt32LE(cd + 24);
  const name = buf.toString("utf8", cd + 46, cd + 46 + buf.readUInt16LE(cd + 28));
  const local = buf.readUInt32LE(cd + 42);
  if (compSize === 0xffffffff || size === 0xffffffff) throw new Error("zip: zip64 not supported");
  if (buf.readUInt32LE(local) !== 0x04034b50) throw new Error("zip: bad local header");
  const start = local + 30 + buf.readUInt16LE(local + 26) + buf.readUInt16LE(local + 28);
  const raw = buf.subarray(start, start + compSize);
  let data;
  if (method === 8) data = inflateRawSync(raw);
  else if (method === 0) data = Buffer.from(raw);
  else throw new Error(`zip: method ${method} not supported`);
  if (data.length !== size) throw new Error(`zip: ${name} is ${data.length} bytes, expected ${size}`);
  if (crc32(data) !== crc) throw new Error(`zip: ${name} CRC mismatch`);
  return { name, data };
}

// Some archive days quote their fields ("" for an empty ratio); none has a comma inside a field.
const unquote = (f) => {
  const v = f.trim();
  return v.length >= 2 && v.startsWith('"') && v.endsWith('"') ? v.slice(1, -1) : v;
};

/** Split CSV text into rows of fields and the header (null when the first line starts with a number). */
export function csv(text) {
  const lines = text.split(/\r?\n/).filter((l) => l.trim() !== "");
  if (!lines.length) return { header: null, rows: [] };
  const split = (l) => l.split(",").map(unquote);
  const first = split(lines[0]);
  const header = Number.isFinite(Number(first[0])) && first[0].trim() !== "" ? null : first.map((h) => h.trim());
  return { header, rows: (header ? lines.slice(1) : lines).map(split) };
}

const num = (s) => {
  if (s === undefined || s.trim() === "") return null;
  const x = Number(s);
  if (!Number.isFinite(x)) throw new Error(`not a number: ${s}`);
  return x;
};
// Some Binance archives moved to microsecond times; keep milliseconds throughout.
const ms = (s) => {
  const x = num(s);
  return x > 1e14 ? Math.floor(x / 1000) : x;
};
const sortDedupe = (rows) => [...new Map(rows.map((r) => [r[0], r])).values()].sort((a, b) => a[0] - b[0]);

/** Five-minute klines CSV, with or without a header, to [openMs, o, h, l, c, volume, quoteVolume, trades, takerBuyVolume]. */
export function parseKlines(text) {
  const { rows } = csv(text);
  return sortDedupe(rows.map((f) => [ms(f[0]), num(f[1]), num(f[2]), num(f[3]), num(f[4]), num(f[5]), num(f[7]), num(f[8]), num(f[9])]));
}

/** Monthly fundingRate CSV (calc_time, funding_interval_hours, last_funding_rate) to [fundingTimeMs, rate]. */
export function parseFunding(text) {
  const { header, rows } = csv(text);
  const at = (name, d) => (header ? header.indexOf(name) : d);
  const t = at("calc_time", 0);
  const r = at("last_funding_rate", 2);
  if (t < 0 || r < 0) throw new Error(`funding: unexpected header ${header}`);
  return sortDedupe(rows.map((f) => [ms(f[t]), num(f[r])]));
}

/**
 * Daily metrics CSV (create_time "YYYY-MM-DD HH:MM:SS" UTC, symbol, sum_open_interest, sum_open_interest_value, and
 * the long/short ratio columns) to [ms, sumOpenInterest, sumOpenInterestValue, ...ratios]; a ratio the file lacks is
 * null. With cols "oi" the ratios are left out; with "sumoi" only [ms, sumOpenInterest] is kept.
 */
export function parseMetrics(text, cols = "full") {
  if (!METRIC_COLS.includes(cols)) throw new Error(`unknown metrics cols ${cols}`);
  const { header, rows } = csv(text);
  const std = ["create_time", "symbol", "sum_open_interest", "sum_open_interest_value", ...METRIC_SRC];
  const h = header ?? std;
  const idx = (name) => h.indexOf(name);
  const [t, oi, oiv] = [idx("create_time"), idx("sum_open_interest"), idx("sum_open_interest_value")];
  if (t < 0 || oi < 0 || (oiv < 0 && cols !== "sumoi")) throw new Error(`metrics: unexpected header ${header}`);
  const ratios = cols === "full" ? METRIC_SRC.map(idx) : [];
  const time = (s) => {
    const v = s.trim();
    return /^\d+$/.test(v) ? ms(v) : Date.parse(`${v.replace(" ", "T")}Z`);
  };
  return sortDedupe(
    rows.map((f) => {
      const at = time(f[t]);
      if (!Number.isFinite(at)) throw new Error(`metrics: bad time ${f[t]}`);
      if (cols === "sumoi") return [at, num(f[oi])];
      return [at, num(f[oi]), num(f[oiv]), ...ratios.map((i) => (i < 0 ? null : num(f[i])))];
    }),
  );
}

/** Fetch an archive and its .CHECKSUM, check the sha256 and unzip. Null when the archive has no such file (404). */
export function makeArchive({ fetchImpl = fetch, timeoutMs = 60_000, retries = 3, wait = sleep } = {}) {
  let calls = 0;
  async function get(url) {
    for (let attempt = 0; ; attempt += 1) {
      const ctrl = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), timeoutMs);
      try {
        calls += 1;
        const res = await fetchImpl(url, { headers: { "user-agent": "faith-tape-recorder/1" }, signal: ctrl.signal });
        if (res.status === 404) return null;
        if (res.status !== 200) {
          const err = new Error(`HTTP ${res.status} ${new URL(url).pathname}`);
          err.status = res.status;
          throw err;
        }
        return Buffer.from(await res.arrayBuffer());
      } catch (e) {
        const retry = e.status === undefined || e.status === 429 || e.status >= 500;
        if (!retry || attempt >= retries) throw e;
        await wait(1000 * 2 ** attempt);
      } finally {
        clearTimeout(timer);
      }
    }
  }
  async function fetchCsv(url) {
    const zip = await get(url);
    if (!zip) return null;
    const sum = await get(`${url}.CHECKSUM`);
    if (!sum) throw new Error(`no checksum for ${new URL(url).pathname}`);
    const want = sum.toString().trim().split(/\s+/)[0].toLowerCase();
    if (sha256(zip) !== want) throw new Error(`checksum mismatch ${new URL(url).pathname}`);
    return unzipSingle(zip).data.toString("utf8");
  }
  return { fetchCsv, get calls() { return calls; } };
}

const gz = (rows) => gzipSync(rows.map((r) => JSON.stringify(r)).join("\n") + "\n", { level: 9 });

/** Run `fn` over items with at most `n` in flight, results in order. */
async function pool(items, n, fn) {
  const out = new Array(items.length);
  let next = 0;
  await Promise.all(
    Array.from({ length: Math.min(n, items.length) }, async () => {
      while (next < items.length) {
        const i = next++;
        out[i] = await fn(items[i]);
      }
    }),
  );
  return out;
}

const DIRS = { klines: "klines5m", funding: "funding", metrics: "metrics" };

export async function backfill({
  root,
  syms,
  kinds = KINDS,
  from = "2021-01",
  to = HOLDOUT,
  metricsFrom = from,
  metricsCols = "full",
  fetchImpl,
  wait,
  concurrency = 8,
  log = console.log,
}) {
  checkRange(from, to);
  for (const k of kinds) if (!KINDS.includes(k)) throw new Error(`unknown kind ${k}`);
  if (!METRIC_COLS.includes(metricsCols)) throw new Error(`unknown metrics cols ${metricsCols}`);
  const a = makeArchive({ fetchImpl, wait });
  const out = { written: 0, skipped: 0, failed: [], missing: {} };
  for (const sym of syms) {
    if (!/^[A-Z0-9]+$/.test(sym)) throw new Error(`bad symbol ${sym}`);
    for (const kind of kinds) {
      const dir = join(root, "data", "binance-um", DIRS[kind], sym);
      const missingPath = join(dir, "missing.json");
      const missing = new Set(existsSync(missingPath) ? JSON.parse(readFileSync(missingPath, "utf8")).missing : []);
      const write = (name, rows, note) => {
        mkdirSync(dir, { recursive: true });
        writeFileSync(join(dir, name), gz(rows));
        out.written += 1;
        log(`${sym} ${kind} ${note}: ${rows.length} rows`);
      };
      const months = [];
      for (let ym = kind === "metrics" && metricsFrom > from ? metricsFrom : from; ym < to; ym = nextMonth(ym)) months.push(ym);

      if (kind === "klines") {
        for (const ym of months) {
          if (existsSync(join(dir, `${ym}.ndjson.gz`))) { out.skipped += 1; continue; }
          try {
            const text = await a.fetchCsv(`${BASE}/monthly/klines/${sym}/5m/${sym}-5m-${ym}.zip`);
            if (text === null) { missing.add(ym); continue; }
            const rows = parseKlines(text).filter((r) => r[0] >= monthStart(ym) && r[0] < monthStart(nextMonth(ym)));
            if (!rows.length) { missing.add(ym); continue; }
            write(`${ym}.ndjson.gz`, rows, ym);
            missing.delete(ym);
          } catch (e) {
            out.failed.push(`${sym} ${kind} ${ym}: ${e.message}`);
            log(`${sym} ${kind} ${ym} failed: ${e.message}`);
          }
        }
      }

      if (kind === "funding") {
        const years = [...new Set(months.map((m) => m.slice(0, 4)))];
        for (const y of years) {
          if (existsSync(join(dir, `${y}.ndjson.gz`))) { out.skipped += 1; continue; }
          const ms12 = months.filter((m) => m.startsWith(y));
          const rows = [];
          let ok = true;
          for (const ym of ms12) {
            try {
              const text = await a.fetchCsv(`${BASE}/monthly/fundingRate/${sym}/${sym}-fundingRate-${ym}.zip`);
              if (text === null) { missing.add(ym); continue; }
              rows.push(...parseFunding(text).filter((r) => r[0] >= monthStart(ym) && r[0] < monthStart(nextMonth(ym))));
              missing.delete(ym);
            } catch (e) {
              ok = false;
              out.failed.push(`${sym} ${kind} ${ym}: ${e.message}`);
              log(`${sym} ${kind} ${ym} failed: ${e.message}`);
            }
          }
          // A partial year (range cut mid-year) is not written either, so a later full run is not skipped.
          if (ok && ms12.length === 12 && rows.length) write(`${y}.ndjson.gz`, sortDedupe(rows), y);
        }
      }

      if (kind === "metrics") {
        for (const ym of months) {
          if (existsSync(join(dir, `${ym}.ndjson.gz`))) { out.skipped += 1; continue; }
          const days = daysOf(ym);
          let failed = null;
          const parsed = await pool(days, concurrency, async (d) => {
            try {
              const text = await a.fetchCsv(`${BASE}/daily/metrics/${sym}/${sym}-metrics-${d}.zip`);
              return text === null ? null : parseMetrics(text, metricsCols);
            } catch (e) {
              failed ??= `${d}: ${e.message}`;
              return undefined;
            }
          });
          if (failed) {
            out.failed.push(`${sym} ${kind} ${failed}`);
            log(`${sym} ${kind} ${ym} failed: ${failed}`);
            continue;
          }
          const gone = days.filter((d, i) => parsed[i] === null);
          const rows = sortDedupe(parsed.filter((p) => p).flat().filter((r) => r[0] >= monthStart(ym) && r[0] < monthStart(nextMonth(ym))));
          for (const d of days) missing.delete(d);
          missing.delete(ym);
          if (gone.length === days.length || !rows.length) { missing.add(ym); continue; }
          for (const d of gone) missing.add(d);
          write(`${ym}.ndjson.gz`, rows, ym + (gone.length ? ` (${gone.length} days missing)` : ""));
        }
      }

      const list = [...missing].sort();
      if (list.length) {
        mkdirSync(dir, { recursive: true });
        writeFileSync(missingPath, JSON.stringify({ note: "months (YYYY-MM) or days (YYYY-MM-DD) the archive did not have", missing: list }, null, 1) + "\n");
        out.missing[`${sym} ${kind}`] = list;
      }
    }
  }
  out.calls = a.calls;
  return out;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const args = process.argv.slice(2);
  const opt = (k, d) => (args.includes(k) ? args[args.indexOf(k) + 1] : d);
  const root = resolve(args.find((a, i) => !a.startsWith("--") && !args[i - 1]?.startsWith("--")) ?? ".");
  const from = opt("--from", "2021-01");
  const to = opt("--to", HOLDOUT);
  const metricsFrom = opt("--metrics-from", from);
  checkRange(from, to); // throws for any month after 2025-12
  checkRange(metricsFrom, to);
  const out = await backfill({
    root,
    syms: opt("--syms", "BTCUSDT,ETHUSDT,SOLUSDT,DOGEUSDT").split(","),
    kinds: opt("--kinds", KINDS.join(",")).split(","),
    from,
    to,
    metricsFrom,
    metricsCols: opt("--metrics-cols", "full"),
  });
  console.log(JSON.stringify(out));
  if (out.failed.length) process.exitCode = 1;
}
