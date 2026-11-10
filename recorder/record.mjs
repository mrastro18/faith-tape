#!/usr/bin/env node
// One recorder run: catch every stream up from its cursor, write data/ and state/, and a health line.
// Usage: node recorder/record.mjs [rootDir]
// Safe to run any time and as often as wanted: cursors make every stream resume exactly where it stopped.
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { readFileSync, existsSync, mkdirSync, writeFileSync } from "node:fs";
import { appendByDay, dayOf, makeDeadline, makeFetcher, readJson, secondBars, sha256, sleep, writeJson } from "./lib.mjs";
import * as bus from "./venues/binanceus.mjs";
import * as cb from "./venues/coinbase.mjs";
import * as kr from "./venues/kraken.mjs";
import * as ks from "./venues/kalshi.mjs";

const SETTLE_MS = 5000; // a second is final once the fetch happened this long after it ended

/** {"YYYY-MM": mean trades per day} over completed daily candles [openMs, close, quoteVol, trades]. */
export function monthlyTradesPerDay(rows) {
  const acc = {};
  for (const r of rows) {
    const m = new Date(r[0]).toISOString().slice(0, 7);
    (acc[m] ||= [0, 0])[0] += r[3];
    acc[m][1] += 1;
  }
  return Object.fromEntries(Object.entries(acc).map(([m, [s, n]]) => [m, Math.round(s / n)]));
}

export async function runOnce({ root, config, fetchImpl, now = Date.now, wait = sleep, log = console.log }) {
  const startedAt = now();
  const deadline = makeDeadline(config.budgetSeconds * 1000, now);
  const f = makeFetcher({ fetchImpl, spacingMs: config.spacingMs, now, wait });
  const statePath = join(root, "state", "state.json");
  const state = readJson(statePath, { version: 1, binanceus: {}, coinbase: {}, kraken: {}, kalshi: {}, daily: {} });
  const today = dayOf(startedAt);
  const startMs = Date.parse(config.start);
  const D = (...p) => join(root, "data", ...p);
  const health = { at: new Date(startedAt).toISOString(), run: process.env.GITHUB_RUN_ID ?? "local", tasks: {} };
  const gaps = [];

  async function task(name, fn) {
    const t0 = now();
    try {
      const out = await fn();
      health.tasks[name] = { ok: true, ms: now() - t0, ...out };
    } catch (e) {
      health.tasks[name] = { ok: false, ms: now() - t0, error: String(e.message ?? e).slice(0, 300), status: e.status ?? null };
      log(`task ${name} failed: ${e.message}`);
    }
  }

  // ---------- Binance.US: the venue under study ----------
  async function binanceus() {
    const s = (state.binanceus ||= {});
    if (s.pairsDay !== today || !s.pairs) {
      s.pairs = await bus.listPairs(f, config.binanceus.quotes);
      s.pairsDay = today;
      writeJson(D("binanceus", "pairs", `${today}.json`), s.pairs);
    }
    s.cursors ||= {};
    const pauseIfHeavy = async (used) => {
      if (used > config.binanceus.weightPause) {
        const ms = 61000 - (now() % 60000);
        log(`binance.us weight ${used}, pausing ${ms} ms`);
        await wait(ms);
      }
    };
    const t = now();
    const listed = new Set(s.pairs.map((p) => p.symbol));
    const stats = (await bus.stats24h(f)).filter((r) => listed.has(r[0]));
    appendByDay((d) => D("binanceus", "stats24h", `${d}.ndjson`), [[t, stats]], (r) => dayOf(r[0]));
    // Universe (CHANGES.md, 2026-11-10b): a pair is recorded when it is thin the first time it is seen, and
    // stops only if it becomes very busy. The rule reads nothing but the 24 h count at that moment.
    const u = config.binanceus.universe;
    const counts = new Map(stats.map((r) => [r[0], r[1]]));
    s.universe ||= {};
    for (const p of s.pairs) {
      const n = counts.get(p.symbol) ?? 0;
      const cur = s.universe[p.symbol];
      if (!cur) s.universe[p.symbol] = n <= u.maxCountAtEntry ? { in: true, day: today, count: n } : { in: false, day: today, count: n, why: "busy when first seen" };
      else if (cur.in && n > u.dropAbove) s.universe[p.symbol] = { in: false, day: today, count: n, why: "busy", since: cur.day };
    }
    const tracked = s.pairs.filter((p) => s.universe[p.symbol].in);
    // Once a day, daily candles for every listed pair: the trade counts that later universe rules read.
    let klineSymbols = 0;
    if (s.klinesDay !== today) {
      if (s.klinesProgress?.day !== today) s.klinesProgress = { day: today, done: {} };
      s.activity ||= {};
      for (const p of s.pairs) {
        if (s.klinesProgress.done[p.symbol]) continue;
        if (deadline.passed()) break;
        const rows = await bus.klines1d(f, p.symbol, startMs, now());
        writeJson(D("binanceus", "klines1d", `${p.symbol}.json`), rows);
        s.activity[p.symbol] = monthlyTradesPerDay(rows);
        s.klinesProgress.done[p.symbol] = 1;
        klineSymbols += 1;
      }
      if (s.pairs.every((p) => s.klinesProgress.done[p.symbol])) {
        s.klinesDay = today;
        delete s.klinesProgress;
      }
    }
    let depthRows = 0;
    const depths = [];
    for (const p of tracked) {
      if (deadline.passed()) break;
      try {
        depths.push([p.symbol, ...(await bus.depth(f, p.symbol, config.binanceus.depthLevels, now()))]);
      } catch (e) {
        if (e.status === 429 || e.status === 418) throw e;
      }
    }
    depthRows = appendByDay((d) => D("binanceus", "depth", `${d}.ndjson`), depths, (r) => dayOf(r[1]));
    let written = 0;
    let behind = 0;
    for (const passPages of [config.binanceus.firstPassPages, config.binanceus.maxPagesPerSymbol]) {
      behind = 0;
      for (const p of tracked) {
        if (deadline.passed()) {
          behind += 1;
          continue;
        }
        const c = s.cursors[p.symbol];
        if (c && c.caughtUp && passPages !== config.binanceus.firstPassPages) continue;
        let cursor = c?.id;
        if (cursor == null) {
          const first = await bus.firstIdAtOrAfter(f, p.symbol, startMs);
          if (first == null) {
            s.cursors[p.symbol] = { id: null, caughtUp: true, empty: true };
            continue;
          }
          cursor = first - 1;
        }
        const r = await bus.pullTrades(f, p.symbol, cursor, { deadline, maxPages: passPages, onWeight: pauseIfHeavy });
        written += appendByDay((d) => D("binanceus", "trades", d, `${p.symbol}.ndjson`), r.rows, (row) => dayOf(row[1]));
        s.cursors[p.symbol] = { id: r.cursor, caughtUp: r.caughtUp, lastMs: r.rows.length ? r.rows[r.rows.length - 1][1] : c?.lastMs ?? null };
        if (!r.caughtUp) behind += 1;
      }
    }
    return { pairs: s.pairs.length, tracked: tracked.length, klineSymbols, depthRows, tradesWritten: written, symbolsBehind: behind };
  }

  // ---------- Reference prices: Coinbase and Kraken, one-second bars ----------
  function referenceBases() {
    const bases = new Set(config.reference.always);
    const uni = state.binanceus?.universe ?? {};
    for (const p of state.binanceus?.pairs ?? []) if (uni[p.symbol]?.in) bases.add(p.base);
    return [...bases].sort();
  }

  // Bases that get one-minute history before recording began: the always set, then every base with a
  // Binance.US pair that averaged at least `historyMinTradesPerDay` trades a day in some calendar month.
  // A superset of anything a monthly universe rule could pick, decided from trade counts alone.
  function historyBases() {
    const always = config.reference.always;
    const min = config.reference.historyMinTradesPerDay;
    const best = new Map();
    for (const p of state.binanceus?.pairs ?? []) {
      const months = state.binanceus?.activity?.[p.symbol] ?? {};
      const top = Math.max(0, ...Object.values(months));
      if (top >= min) best.set(p.base, Math.max(best.get(p.base) ?? 0, top));
    }
    const rest = [...best.keys()].filter((b) => !always.includes(b)).sort((a, b) => best.get(b) - best.get(a) || (a < b ? -1 : 1));
    return [...always, ...rest];
  }

  async function coinbase() {
    const s = (state.coinbase ||= {});
    if (s.productsDay !== today || !s.products) {
      s.products = await cb.usdProducts(f);
      s.productsDay = today;
    }
    s.cursors ||= {};
    s.recordStart ||= {};
    s.candlesUntil ||= {};
    const bases = referenceBases().filter((b) => s.products[b]);
    let bars = 0;
    for (const base of bases) {
      if (deadline.passed()) break;
      const product = s.products[base];
      const fetchedAt = now();
      const r = await cb.tradesSince(f, product, s.cursors[base], { maxPages: config.reference.coinbaseMaxPages, deadline });
      if (r.gap && r.trades.length) gaps.push({ venue: "coinbase", key: product, afterId: r.gapFrom, beforeId: r.gapTo, at: fetchedAt });
      const cutoffSec = Math.floor((fetchedAt - SETTLE_MS) / 1000);
      const final = r.trades.filter((t) => Math.floor(t.t / 1000) < cutoffSec);
      // On the very first run only seconds after the run start are kept, so the first bar is never partial.
      const keep = s.cursors[base] == null ? final.filter((t) => t.t >= fetchedAt - 60000) : final;
      const rows = secondBars(keep, cutoffSec);
      bars += appendByDay((d) => D("coinbase", "bars1s", d, `${base}.ndjson`), rows, (row) => dayOf(row[0] * 1000));
      if (final.length) s.cursors[base] = final[final.length - 1].id;
      else if (s.cursors[base] == null && r.trades.length) s.cursors[base] = r.trades[0].id - 1;
      if (s.recordStart[base] == null) s.recordStart[base] = rows.length ? rows[0][0] * 1000 : fetchedAt;
    }
    // History before recording began: one-minute candles, oldest first, a bounded slice per run.
    let candles = 0;
    const chunk = config.reference.candleChunkMinutes * 60000;
    const hist = historyBases().filter((b) => s.products[b] && s.recordStart[b] != null);
    for (const base of hist) {
      const until = Math.floor(s.recordStart[base] / 60000) * 60000;
      let from = s.candlesUntil[base] ?? startMs;
      while (from < until && !deadline.passed()) {
        const to = Math.min(from + chunk, until);
        const rows = await cb.candles1m(f, s.products[base], from, to);
        candles += appendByDay((d) => D("coinbase", "candles1m", d, `${base}.ndjson`), rows, (row) => dayOf(row[0]));
        from = to;
        s.candlesUntil[base] = from;
      }
    }
    return { bases: bases.length, historyBases: hist.length, bars, candles };
  }

  async function kraken() {
    const s = (state.kraken ||= {});
    if (s.pairsDay !== today || !s.pairs) {
      s.pairs = await kr.usdPairs(f);
      s.pairsDay = today;
    }
    s.cursors ||= {};
    const always = new Set(config.reference.always);
    const bases = referenceBases()
      .filter((b) => s.pairs[b])
      .sort((a, b) => (always.has(b) ? 1 : 0) - (always.has(a) ? 1 : 0))
      .slice(0, config.reference.krakenMax);
    let bars = 0;
    for (const base of bases) {
      if (deadline.passed()) break;
      const fetchedAt = now();
      const st = s.cursors[base];
      const r = await kr.tradesSince(f, s.pairs[base], st, { deadline, startSinceNs: String(BigInt(fetchedAt - 60000) * 1000000n) });
      const cutoffSec = r.caughtUp ? Math.floor((fetchedAt - SETTLE_MS) / 1000) : r.trades.length ? Math.floor(r.trades[r.trades.length - 1].t / 1000) : 0;
      const final = r.trades.filter((t) => Math.floor(t.t / 1000) < cutoffSec);
      const rows = secondBars(final, cutoffSec);
      bars += appendByDay((d) => D("kraken", "bars1s", d, `${base}.ndjson`), rows, (row) => dayOf(row[0] * 1000));
      const lastId = final.length ? final[final.length - 1].id : st?.lastId ?? -1;
      // Resume one second before the cutoff; ids at or below lastId are dropped, so the overlap is harmless.
      s.cursors[base] = { since: String(BigInt(Math.max(cutoffSec - 1, 0)) * 1000000000n), lastId };
    }
    return { bases: bases.length, bars };
  }

  // ---------- Kalshi perpetuals: one snapshot of the crypto markets each run ----------
  async function kalshi() {
    const at = now();
    const { markets, pages } = await ks.marginMarkets(f);
    const classes = {};
    for (const m of markets) classes[m.asset_class ?? "?"] = (classes[m.asset_class ?? "?"] ?? 0) + 1;
    const keep = markets.filter((m) => /crypto/i.test(String(m.asset_class ?? "")));
    if (keep.length) appendByDay((d) => D("kalshi", "perps", `${d}.ndjson`), [[at, keep]], (r) => dayOf(r[0]));
    return { markets: markets.length, crypto: keep.length, pages, classes };
  }

  // ---------- Once a day: endpoint probes and fee/terms page hashes ----------
  async function daily() {
    const s = (state.daily ||= {});
    if (s.day === today) return { skipped: true };
    const out = [];
    for (const kind of ["probes", "pages"]) {
      for (const item of config[kind]) {
        if (deadline.passed()) break;
        try {
          const r = await f.getText(item.url);
          const hash = sha256(r.text);
          const rec = { at: now(), kind, name: item.name, url: item.url, status: r.status, bytes: r.text.length, sha256: hash, head: kind === "probes" ? r.text.slice(0, 300) : undefined };
          out.push(rec);
          if (kind === "pages") {
            const prev = s.hashes?.[item.name];
            if (prev !== hash) {
              const p = D("pages", item.name, `${today}.html`);
              mkdirSync(join(p, ".."), { recursive: true });
              writeFileSync(p, r.text.slice(0, 2_000_000));
              rec.changed = prev != null;
            }
            (s.hashes ||= {})[item.name] = hash;
          }
        } catch (e) {
          out.push({ at: now(), kind, name: item.name, url: item.url, error: String(e.message ?? e).slice(0, 200) });
        }
      }
    }
    appendByDay((d) => D("probes", `${d}.ndjson`), out, (r) => dayOf(r.at));
    s.day = today;
    return { items: out.length };
  }

  // Each host runs its own queue, so a slow venue never starves another.
  await Promise.all([task("binanceus", binanceus), task("coinbase", coinbase), task("kraken", kraken), task("kalshi", kalshi), task("daily", daily)]);

  if (gaps.length) appendByDay((d) => D("gaps", `${d}.ndjson`), gaps, (g) => dayOf(g.at));
  health.calls = f.calls;
  health.http = f.statuses.slice(0, 50);
  health.ms = now() - startedAt;
  appendByDay((d) => D("health", `${d}.ndjson`), [health], () => today);
  writeJson(statePath, state);
  writeJson(join(root, "state", "last-run.json"), health);
  return health;
}

const isMain = process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1]);
if (isMain) {
  const root = process.argv[2] ?? process.cwd();
  const config = JSON.parse(readFileSync(new URL("./config.json", import.meta.url), "utf8"));
  if (!existsSync(join(root, "state"))) mkdirSync(join(root, "state"), { recursive: true });
  const h = await runOnce({ root, config });
  console.log(JSON.stringify(h, null, 1));
  // A run where every venue failed is a failed job, so GitHub marks it red; partial failures stay green
  // and show in data/health.
  const all = Object.entries(h.tasks).filter(([k]) => k !== "daily" && k !== "kalshi");
  if (all.every(([, v]) => !v.ok)) process.exit(1);
}
