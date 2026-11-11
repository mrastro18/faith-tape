#!/usr/bin/env node
// Aggregates the day files written by run.mjs into small summaries (the only files committed):
//   node tools/maker-sim/agg.mjs --in <dir of *.json.gz> --out <dir> [--missing missing.tsv] [--meta k=v ...]
// Writes summary.json (per venue and pair: every variant, cancellation model and side, with a split by
// fill type), days.json (one line per pair and day) and manifest.json (inputs, hashes, what was missing).
// Means carry a descriptive 90% interval by a cluster bootstrap over UTC days (queue.mjs describe); the
// days are a month apart and few, so the intervals show noise and are not inference.
import { existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { gunzipSync } from "node:zlib";
import { describe } from "./queue.mjs";
import { quantile } from "./stats.mjs";
import { FILL, HOW, ORDER_FIELDS, STATUS, TICK } from "./tick.mjs";

/**
 * Lowest public retail tier, in bp, read 2026-11-11:
 *  - Coinbase Advanced: help.coinbase.com/en/exchange/trading-and-funding/exchange-fees gives a maker fee
 *    "between 0.00% and 0.40%" and a taker fee "between 0.05% and 0.60%"; the top of each range is the
 *    lowest volume tier ($0 - $10K on help.coinbase.com/en/pro/trading-and-funding/trading-rules-and-fees/fees).
 *  - Kraken Pro: kraken.com/pro/fees, Tier 1 ($0+): maker 0.40%, taker 0.80% (tiers changed 2026-07-09,
 *    support.kraken.com/gb/articles/cross-platform-fee-tier-changes). An "eligible discounted pairs" 0.02%
 *    discount exists for a list not readable as text; it is not applied.
 */
export const FEES = Object.freeze({
  coinbase: { makerBp: 40, takerBp: 60 },
  kraken: { makerBp: 40, takerBp: 80 },
});
const SUM = { resamples: 2000, summaryMinClusters: 10 };
const F = Object.fromEntries(ORDER_FIELDS.map((k, i) => [k, i])); // field index by name
const pct = (xs, p) => (xs.length ? quantile([...xs].sort((a, b) => a - b), p) : null);
const share = (a, b) => (b > 0 ? a / b : null);
const round = (x, d = 4) => (x == null || !Number.isFinite(x) ? x : Math.round(x * 10 ** d) / 10 ** d);
const tidy = (o) =>
  JSON.parse(JSON.stringify(o, (k, v) => (typeof v === "number" ? round(v) : v)));

const items = (rows, f, day) => {
  const out = [];
  for (const r of rows) {
    const v = f(r);
    if (v != null && Number.isFinite(v)) out.push({ value: v, cluster: day(r) });
  }
  return out;
};
const desc = (rows, f, day) => {
  const d = describe(items(rows, f, day), SUM);
  return d.n ? { n: d.n, mean: d.mean, median: d.median, lo90: d.lo90, hi90: d.hi90, days: d.hours } : { n: 0 };
};

/** Net of fees: entry as a maker; exit as a maker (maker fee) or as a taker (taker fee). */
export function nets(r, fee) {
  const tg = r[F.takerGross];
  const mg = r[F.makerGross];
  const how = HOW[r[F.makerHow]];
  return {
    takerNet: tg == null ? null : tg - fee.makerBp - fee.takerBp,
    makerNet: mg == null ? null : how === "maker" ? mg - 2 * fee.makerBp : mg - fee.makerBp - fee.takerBp,
  };
}

/** Summary of one group of order rows (any mix of sides). `day(r)` gives the row's UTC day. */
export function summarize(rows, fee, day) {
  const counts = {};
  for (const r of rows) counts[STATUS[r[F.status]]] = (counts[STATUS[r[F.status]]] ?? 0) + 1;
  const filled = rows.filter((r) => STATUS[r[F.status]] === "filled");
  const known = (counts.filled ?? 0) + (counts.expired ?? 0) + (counts.expiredPartial ?? 0);
  const fillWithin = Object.fromEntries(
    TICK.fillWithinMin.map((h) => [
      `${h}m`,
      share(filled.filter((r) => r[F.ttfSec] <= h * 60).length, known),
    ]),
  );
  const fillTypes = Object.fromEntries(FILL.map((t, i) => [t, filled.filter((r) => r[F.fillType] === i).length]));
  const typeShare = Object.fromEntries(FILL.map((t) => [t, share(fillTypes[t], filled.length)]));
  const ahead = rows.map((r) => r[F.ahead0Quote]).filter((x) => x != null);
  const metric = (rs, name) => desc(rs, (r) => r[F[name]], day);
  const block = (rs) => {
    const wn = rs.map((r) => ({ r, ...nets(r, fee) }));
    return {
      n: rs.length,
      halfSpreadAtFill: metric(rs, "edge0"),
      midMove: { "1m": metric(rs, "mm1"), "5m": metric(rs, "mm5"), "15m": metric(rs, "mm15") },
      edgeVsMid: { "1m": metric(rs, "e1"), "5m": metric(rs, "e5"), "15m": metric(rs, "e15") },
      takerExit5m: {
        gross: metric(rs, "takerGross"),
        net: desc(wn, (x) => x.takerNet, (x) => day(x.r)),
      },
      makerExit: {
        how: Object.fromEntries(HOW.map((h, i) => [h, rs.filter((r) => r[F.makerHow] === i).length])),
        gross: metric(rs, "makerGross"),
        net: desc(wn, (x) => x.makerNet, (x) => day(x.r)),
      },
    };
  };
  const spreadBucket = (t) => (t <= 1 ? "1 tick" : t <= 4 ? "2-4 ticks" : "5+ ticks");
  const bySpread = {};
  for (const r of rows) {
    if (r[F.spreadTicks] == null) continue;
    const b = (bySpread[spreadBucket(r[F.spreadTicks])] ??= { placed: 0, filled: 0, known: 0, spreadBp: [], fills: [] });
    b.placed++;
    b.spreadBp.push(r[F.spreadBp]);
    const st = STATUS[r[F.status]];
    if (st === "filled" || st === "expired" || st === "expiredPartial") b.known++;
    if (st === "filled") {
      b.filled++;
      b.fills.push(r);
    }
  }
  return {
    counts,
    fillWithin,
    unknownShare: share(counts.unknown ?? 0, known + (counts.unknown ?? 0)),
    fillTypes,
    fillTypeShare: typeShare,
    takerDrivenShare: share(fillTypes.queue + fillTypes.through, filled.length),
    toleranceUsedShare: share(filled.filter((r) => r[F.adj] === 1).length, filled.length),
    timeToFillSec: { p50: pct(filled.map((r) => r[F.ttfSec]), 0.5), p90: pct(filled.map((r) => r[F.ttfSec]), 0.9) },
    queueAheadQuote: { p50: pct(ahead, 0.5), p90: pct(ahead, 0.9), zeroShare: share(ahead.filter((x) => x === 0).length, ahead.length) },
    all: block(filled),
    byFillType: Object.fromEntries(FILL.map((t, i) => [t, block(filled.filter((r) => r[F.fillType] === i))])),
    bySpread: Object.fromEntries(
      Object.entries(bySpread).map(([k, b]) => [
        k,
        {
          placed: b.placed,
          fillRate15m: share(b.filled, b.known),
          spreadBpMedian: pct(b.spreadBp, 0.5),
          midMove5m: metric(b.fills, "mm5"),
          makerExitNet: desc(b.fills.map((r) => ({ r, ...nets(r, fee) })), (x) => x.makerNet, (x) => day(x.r)),
        },
      ]),
    ),
  };
}

function main() {
  const arg = (k) => {
    const i = process.argv.indexOf(k);
    return i > 0 ? process.argv[i + 1] : undefined;
  };
  const inDir = arg("--in");
  const outDir = arg("--out");
  const meta = {};
  process.argv.forEach((a, i) => {
    if (a === "--meta") {
      const [k, ...v] = process.argv[i + 1].split("=");
      meta[k] = v.join("=");
    }
  });
  const files = readdirSync(inDir, { recursive: true })
    .filter((f) => String(f).endsWith(".json.gz"))
    .map((f) => join(inDir, String(f)))
    .sort();
  const byPair = new Map();
  for (const f of files) {
    const d = JSON.parse(gunzipSync(readFileSync(f)).toString("utf8"));
    if (d.orderFields.join() !== ORDER_FIELDS.join()) throw new Error(`${f}: order fields differ`);
    const key = `${d.venue}|${d.symbol}`;
    if (!byPair.has(key)) byPair.set(key, []);
    byPair.get(key).push(d);
  }
  const missing = arg("--missing") && existsSync(arg("--missing"))
    ? readFileSync(arg("--missing"), "utf8").split("\n").filter(Boolean).map((l) => {
        const [venue, symbol, day, kind, code] = l.split("\t");
        return { venue, symbol, day, kind, code };
      })
    : [];
  const summary = {};
  const days = [];
  const inputs = [];
  for (const [key, ds] of [...byPair.entries()].sort()) {
    const [venue, symbol] = key.split("|");
    const fee = FEES[venue];
    ds.sort((a, b) => a.day.localeCompare(b.day));
    const rows = [];
    const dayOf = [];
    const control = [];
    const calib = { bid: {}, ask: {} };
    ds.forEach((d, di) => {
      for (const r of d.orders) {
        rows.push(r);
        dayOf.push(di);
      }
      for (const c of d.control) control.push({ c, di });
      for (const side of ["bid", "ask"])
        for (const [k, v] of Object.entries(d.calibration[side] ?? {})) calib[side][k] = (calib[side][k] ?? 0) + v;
      inputs.push({ venue, symbol, day: d.day, files: d.files, tick: d.tick, quotes: d.quotes, trades: d.trades, coverage: d.coverage });
    });
    // day of a row: rows carry no day field, so tag them
    rows.forEach((r, i) => (r.day = dayOf[i]));
    const day = (r) => r.day;
    const cells = {};
    for (let vi = 0; vi < TICK.variants.length; vi++)
      for (let ci = 0; ci < TICK.cancels.length; ci++) {
        const sel = rows.filter((r) => r[F.variant] === vi && r[F.cancels] === ci);
        const k = `${TICK.variants[vi]}|${TICK.cancels[ci]}`;
        cells[k] = {
          both: summarize(sel, fee, day),
          buy: summarize(sel.filter((r) => r[F.side] === 0), fee, day),
          sell: summarize(sel.filter((r) => r[F.side] === 1), fee, day),
        };
      }
    const ctl = (sideIdx) =>
      Object.fromEntries(
        [1, 2, 3].map((j, h) => [
          `${TICK.horizonsMin[h]}m`,
          desc(
            control.filter((x) => sideIdx == null || x.c[0] === sideIdx),
            (x) => x.c[j],
            (x) => x.di,
          ),
        ]),
      );
    const calibShare = Object.fromEntries(
      ["bid", "ask"].map((s) => {
        const n = Object.values(calib[s]).reduce((a, b) => a + b, 0);
        return [s, { n, ...Object.fromEntries(Object.entries(calib[s]).map(([k, v]) => [k, share(v, n)])) }];
      }),
    );
    summary[key] = {
      venue,
      symbol,
      fees: fee,
      days: ds.map((d) => d.day),
      ticks: [...new Set(ds.map((d) => d.tick))],
      placements: ds.reduce((a, d) => a + d.placements, 0),
      control: { buy: ctl(0), sell: ctl(1) },
      calibration: calibShare,
      cells,
    };
    // per day: the registered join/pessimistic cell, both sides
    ds.forEach((d, di) => {
      const sel = rows.filter((r) => r.day === di && r[F.variant] === 0 && r[F.cancels] === 0);
      const s = summarize(sel, fee, day);
      days.push({
        venue,
        symbol,
        day: d.day,
        tick: d.tick,
        quoteRows: d.quotes.kept,
        prints: d.trades.kept,
        coveredMin: d.coverage.coveredMin,
        placements: d.placements,
        joinPess: {
          fillWithin: s.fillWithin,
          unknownShare: s.unknownShare,
          fillTypeShare: s.fillTypeShare,
          midMove5m: s.all.midMove["5m"].mean ?? null,
          makerExitNet: s.all.makerExit.net.mean ?? null,
        },
      });
    });
  }
  mkdirSync(outDir, { recursive: true });
  const stamp = { schema: "faith-tape.maker-sim/1", ...meta, config: TICK, fees: FEES };
  writeFileSync(join(outDir, "summary.json"), JSON.stringify(tidy({ ...stamp, pairs: summary }), null, 1) + "\n");
  writeFileSync(join(outDir, "days.json"), JSON.stringify(tidy({ ...stamp, days }), null, 1) + "\n");
  writeFileSync(join(outDir, "manifest.json"), JSON.stringify(tidy({ ...stamp, inputs, missing }), null, 1) + "\n");
  console.log(`aggregated ${files.length} day files into ${byPair.size} pairs; ${missing.length} missing inputs`);
}

const isMain = process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1]);
if (isMain) main();
