#!/usr/bin/env node
// Forward record of Coinbase Exchange five-minute candles for FAITH's P-OF3 q2 forward check (USDT-USD premium):
// USDT, BTC, ETH, SOL and DOGE against USD. It fetches only whole UTC days from 2026-11-01, the first day after
// FAITH's 2026 holdout (2026-01-01 to 2026-10-31), and refuses any earlier day. One gzip file per base and day,
// data/forward/coinbase/candles5m/<BASE>/<YYYY-MM-DD>.ndjson.gz, rows [ms, open, high, low, close, volume] as
// backfill.mjs writes them; a five minutes with no trade has no row. A day already on disk is skipped, and a day
// is fetched only once it has ended (plus a margin), so a written file is final. Public endpoint, no key.
// Usage: node recorder/forward.mjs [rootDir] [--bases USDT,BTC] [--max-days 40]
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { gzipSync } from "node:zlib";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { makeFetcher } from "./lib.mjs";
import { candles5m } from "./backfill.mjs";

export const FIRST_DAY = "2026-11-01"; // the day after the holdout ends; nothing earlier is fetched
export const BASES = ["USDT", "BTC", "ETH", "SOL", "DOGE"];
export const MARGIN_MS = 15 * 60_000; // a day is fetched only this long after it ends, so late candles are in
const DAY = 86_400_000;
const dayMs = (d) => Date.parse(`${d}T00:00:00Z`);
const dayStr = (ms) => new Date(ms).toISOString().slice(0, 10);

/** Throws if a day lies before FIRST_DAY. */
export function refuseHoldout(day) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) throw new Error(`bad day ${day}`);
  if (day < FIRST_DAY) throw new Error(`${day} is in or before FAITH's 2026 holdout and is not fetched here`);
}

/** Whole UTC days from FIRST_DAY that have ended at least MARGIN_MS before `now`, oldest first. */
export function dueDays(now) {
  const out = [];
  for (let a = dayMs(FIRST_DAY); a + DAY + MARGIN_MS <= now; a += DAY) out.push(dayStr(a));
  return out;
}

export async function forward({ root, bases = BASES, now = Date.now(), maxDays = 40, fetchImpl, spacingMs, log = console.log, wait }) {
  const f = makeFetcher({ fetchImpl, spacingMs, wait });
  const out = { written: 0, skipped: 0, failed: [] };
  const days = dueDays(now);
  for (const base of bases) {
    let done = 0;
    for (const day of days) {
      refuseHoldout(day);
      const dir = join(root, "data", "forward", "coinbase", "candles5m", base);
      const p = join(dir, `${day}.ndjson.gz`);
      if (existsSync(p)) {
        out.skipped += 1;
        continue;
      }
      if (done >= maxDays) break; // a long gap is filled over several runs
      try {
        const rows = await candles5m(f, `${base}-USD`, dayMs(day), dayMs(day) + DAY);
        mkdirSync(dir, { recursive: true });
        writeFileSync(p, gzipSync(rows.map((r) => JSON.stringify(r)).join("\n") + (rows.length ? "\n" : ""), { level: 9 }));
        out.written += 1;
        done += 1;
        log(`${base} ${day}: ${rows.length} candles`);
      } catch (e) {
        out.failed.push(`${base} ${day}: ${e.message}`);
        log(`${base} ${day} failed: ${e.message}`);
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
  const out = await forward({
    root,
    bases: opt("--bases", BASES.join(",")).split(","),
    maxDays: Number(opt("--max-days", "40")),
    spacingMs: config.spacingMs,
  });
  console.log(JSON.stringify(out));
  if (out.failed.length) process.exitCode = 1;
}
