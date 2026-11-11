#!/usr/bin/env node
// One-off history: Coinbase Exchange five-minute candles for a few liquid bases, 2021-01 to 2025-12, for
// FAITH's intraday screens. It stops at 2026-01-01 on purpose: 2026 is FAITH's intraday holdout and is not
// fetched here. One gzip file per base and month, data/coinbase/candles5m/<BASE>/<YYYY-MM>.ndjson.gz, rows
// [ms, open, high, low, close, volume], oldest first; a five minutes with no trade has no row. A month
// already on disk is skipped, so a cut-off run resumes. A month whose fetch failed is not written.
// Usage: node recorder/backfill.mjs [rootDir] [--bases BTC,ETH] [--from 2021-01] [--to 2026-01]
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { gzipSync } from "node:zlib";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { makeFetcher } from "./lib.mjs";

const BASE = "https://api.exchange.coinbase.com";
const STEP = 300_000; // five minutes
const PER = 300; // candles a request

const monthStart = (ym) => Date.UTC(Number(ym.slice(0, 4)), Number(ym.slice(5, 7)) - 1, 1);
const nextMonth = (ym) => {
  const d = new Date(monthStart(ym));
  d.setUTCMonth(d.getUTCMonth() + 1);
  return d.toISOString().slice(0, 7);
};

export async function candles5m(f, product, startMs, endMs) {
  const rows = new Map();
  for (let a = startMs; a < endMs; a += PER * STEP) {
    const b = Math.min(a + PER * STEP, endMs);
    const q = new URLSearchParams({ granularity: "300", start: new Date(a).toISOString(), end: new Date(b).toISOString() });
    const { json } = await f.getJson(`${BASE}/products/${product}/candles?${q}`);
    for (const [t, low, high, open, close, vol] of json) {
      const ms = t * 1000;
      if (ms >= a && ms < b) rows.set(ms, [ms, open, high, low, close, vol]);
    }
  }
  return [...rows.values()].sort((x, y) => x[0] - y[0]);
}

export async function backfill({ root, bases, from, to, fetchImpl, spacingMs, log = console.log, wait }) {
  const f = makeFetcher({ fetchImpl, spacingMs, wait });
  const out = { written: 0, skipped: 0, failed: [] };
  for (const base of bases) {
    for (let ym = from; ym < to; ym = nextMonth(ym)) {
      const p = join(root, "data", "coinbase", "candles5m", base, `${ym}.ndjson.gz`);
      if (existsSync(p)) {
        out.skipped += 1;
        continue;
      }
      try {
        const rows = await candles5m(f, `${base}-USD`, monthStart(ym), monthStart(nextMonth(ym)));
        if (!rows.length) continue; // not listed yet that month
        mkdirSync(join(root, "data", "coinbase", "candles5m", base), { recursive: true });
        writeFileSync(p, gzipSync(rows.map((r) => JSON.stringify(r)).join("\n") + "\n", { level: 9 }));
        out.written += 1;
        log(`${base} ${ym}: ${rows.length} candles`);
      } catch (e) {
        out.failed.push(`${base} ${ym}: ${e.message}`);
        log(`${base} ${ym} failed: ${e.message}`);
      }
    }
  }
  return out;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const args = process.argv.slice(2);
  const opt = (k, d) => (args.includes(k) ? args[args.indexOf(k) + 1] : d);
  const root = resolve(args.find((a, i) => !a.startsWith("--") && !args[i - 1]?.startsWith("--")) ?? ".");
  const config = JSON.parse(readFileSync(join(root, "recorder", "config.json"), "utf8"));
  const to = opt("--to", "2026-01");
  if (to > "2026-01") throw new Error("2026 is FAITH's intraday holdout and is not fetched here");
  const out = await backfill({
    root,
    bases: opt("--bases", "BTC,ETH,SOL,XRP,DOGE").split(","),
    from: opt("--from", "2021-01"),
    to,
    spacingMs: config.spacingMs,
  });
  console.log(JSON.stringify(out));
  if (out.failed.length) process.exitCode = 1;
}
