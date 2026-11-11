#!/usr/bin/env node
// Best bid and ask, polled while the chain waits for its next run: Binance.US for the recorded pairs, and
// Coinbase Exchange and Kraken spot for the products listed in config.json under quotes.venues.
// Usage: node recorder/quotes.mjs [rootDir] --until <ISO time or epoch ms> [--stream quotes|quotes-bridge]
// --stream quotes-bridge writes the same files under data/<venue>/quotes-bridge/: the bridge workflow polls across
// each half-hourly handover, when the chain's own sessions stop for about two minutes (CHANGES 2026-11-12b).
// One call to /api/v3/ticker/bookTicker returns every pair, so a poll costs the same for 1 pair or 260.
// One file per session, data/binanceus/quotes/<day>/<HHMMSS>Z-<run>.ndjson.gz: written as the same name without
// .gz while the session runs, compressed when it ends. Rows:
//   ["s", ms, run, everyMs]                        a polling session starts
//   ["f", ms, symbol, bid, bidQty, ask, askQty]    a full snapshot of every recorded pair (at the start,
//                                                  then every `snapshotEveryMs`)
//   ["q", ms, symbol, bid, bidQty, ask, askQty]    the bid or ask price changed since the last poll
//   ["x", ms, status]                              a poll failed (status null for a timeout or network error)
//   ["c", ms]                                      the session's byte budget ran out: snapshots only from here
//   ["e", ms, polls, failures, capped]             the session ends
// ms is when the answer arrived. Between an "s" and its "e", outside "x" polls and before any "c", every change
// of best price is here at poll resolution: a price that came and went between two polls is not, and a size
// change alone is not a row. Outside sessions nothing is known; a killed session has no "e" row and stays
// uncompressed. A session that crosses midnight ends and restarts at the boundary. The budget (raw bytes) is per
// session, so a cap never costs the same hours of every day. Sessions on 2026-11-10 up to about 18:07 UTC (before
// this format) are one plain 2026-11-10.ndjson.
//
// Coinbase (data/coinbase/quotes/...) and Kraken (data/kraken/quotes/...) use the same files and rows, with their own
// session, budget and failure count, each venue polled by its own loop so a venue that fails or hangs never stops
// another. Coinbase reads /products/{id}/book?level=1 once per product, one after another (so ms is when the
// poll's last answer arrived, under about a second after its first); Kraken reads every pair in one
// /0/public/Ticker call. A product that alone failed in a poll that otherwise worked is
// ["x", ms, status, symbol]. Kraken's own error answers give the error text as status
// (for example "EService:Unavailable"). Symbols are the venue's own names (BTC-USD, XBTUSD).
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { appendFileSync, mkdirSync, readFileSync, unlinkSync, writeFileSync } from "node:fs";
import { gzipSync } from "node:zlib";
import { dayOf, makeFetcher, readJson, sleep } from "./lib.mjs";
import { book1 } from "./venues/coinbase.mjs";
import { tickers } from "./venues/kraken.mjs";

const URL_ALL = "https://api.binance.us/api/v3/ticker/bookTicker";

/** The pairs to record: every listed pair whose base has at least one pair in the universe (all its legs). */
export function quoteSymbols(state) {
  const b = state?.binanceus ?? {};
  const pairs = b.pairs ?? [];
  const u = b.universe ?? {};
  const bases = new Set(pairs.filter((p) => u[p.symbol]?.in).map((p) => p.base));
  return pairs.filter((p) => bases.has(p.base)).map((p) => p.symbol).sort();
}

const quote = (x) => [Number(x.bidPrice), Number(x.bidQty), Number(x.askPrice), Number(x.askQty)];

/**
 * One venue's polling loop and session files, until `untilMs`. `poll()` returns {quotes: [[symbol, bid, bidQty,
 * ask, askQty]], misses: [[symbol, status]]} or throws; `fatal(e)` says whether a failure stops this venue.
 */
async function runTape({ dir, run, everyMs, snapshotEveryMs, maxBytesPerSession, maxFailures, untilMs, poll, fatal, now, wait, log, name }) {
  const fileFor = (t) => join(dir, dayOf(t), `${new Date(t).toISOString().slice(11, 19).replace(/:/g, "")}Z-${run}.ndjson`);

  let rows = [];
  let bytes = 0; // bytes written in the current session
  let cur = null; // the current session's file, uncompressed while it runs
  const flush = () => {
    if (rows.length && cur) {
      mkdirSync(dirname(cur), { recursive: true });
      appendFileSync(cur, rows.map((r) => JSON.stringify(r)).join("\n") + "\n");
    }
    rows = [];
  };
  const seal = () => {
    flush();
    if (!cur) return;
    writeFileSync(`${cur}.gz`, gzipSync(readFileSync(cur), { level: 9 }));
    unlinkSync(cur);
    cur = null;
  };
  const push = (r) => {
    rows.push(r);
    bytes += JSON.stringify(r).length + 1;
  };

  const out = { polls: 0, failures: 0, misses: 0, rows: 0, sessions: 0, capped: false, stopped: null };
  let last = new Map(); // symbol -> [bid, bidQty, ask, askQty]
  let day = null;
  let lastSnap = -Infinity;
  let lastT = null;
  let capped = false;
  let inRow = 0;
  let polls = 0;
  let failures = 0;
  const open = (t) => {
    day = dayOf(t);
    cur = fileFor(t);
    bytes = 0;
    capped = false;
    push(["s", t, run, everyMs]);
    out.sessions += 1;
    last = new Map();
    lastSnap = -Infinity;
    polls = 0;
    failures = 0;
  };
  const close = (t) => {
    push(["e", t, polls, failures, capped]);
    if (capped) out.capped = true;
  };

  let nextAt = now();
  let lastFlush = now();
  while (now() + everyMs <= untilMs) {
    const d = nextAt - now();
    if (d > 0) await wait(d);
    nextAt = Math.max(nextAt + everyMs, now()); // no burst of catch-up polls after a slow one
    let got;
    try {
      got = await poll();
      inRow = 0;
    } catch (e) {
      out.failures += 1;
      if (day) {
        failures += 1;
        push(["x", now(), e.xStatus ?? e.status ?? null]);
      }
      inRow += 1;
      log(`${name} failed: ${e.message}`);
      if (fatal(e) || inRow >= maxFailures) {
        out.stopped = e.status ? `HTTP ${e.status}` : fatal(e) ? e.xStatus ?? "failures" : "failures";
        break;
      }
      continue;
    }
    const t = now();
    if (day && dayOf(t) !== day) {
      close(lastT ?? t);
      out.rows += rows.length;
      seal();
    }
    if (!day || dayOf(t) !== day) open(t);
    polls += 1;
    out.polls += 1;
    lastT = t;
    const full = t - lastSnap >= snapshotEveryMs;
    if (full) lastSnap = t;
    if (!capped && bytes > maxBytesPerSession) {
      capped = true;
      push(["c", t]);
    }
    for (const [symbol, ...q] of got.quotes) {
      const p = last.get(symbol);
      last.set(symbol, q);
      if (full) push(["f", t, symbol, ...q]);
      else if (!capped && (!p || p[0] !== q[0] || p[2] !== q[2])) push(["q", t, symbol, ...q]);
    }
    for (const [symbol, status] of got.misses ?? []) {
      out.misses += 1;
      push(["x", t, status, symbol]);
    }
    if (now() - lastFlush >= 60_000) {
      out.rows += rows.length;
      flush();
      lastFlush = now();
    }
  }
  if (day) close(lastT ?? now());
  out.rows += rows.length;
  seal();
  return out;
}

const httpFatal = (e) => e.status === 429 || e.status === 418 || e.status === 403;

/** Coinbase: one level-1 book call per product. A product that fails alone is a miss; all failing is a failed poll. */
function coinbasePoll(f, products) {
  return async () => {
    const quotes = [];
    const misses = [];
    let lastErr = null;
    for (const product of products) {
      try {
        quotes.push([product, ...(await book1(f, product))]);
      } catch (e) {
        if (httpFatal(e)) throw e; // a rate limit stops the venue at once, not after five more calls
        lastErr = e;
        misses.push([product, e.status ?? null]);
      }
    }
    if (!quotes.length) throw lastErr ?? new Error("coinbase: no products");
    return { quotes, misses };
  };
}

/** Kraken: one Ticker call for every pair. A pair the answer leaves out is a miss with status "missing". */
function krakenPoll(f, pairs) {
  return async () => {
    const { quotes, missing } = await tickers(f, pairs);
    if (!quotes.length) {
      const e = new Error("kraken Ticker: none of the pairs in the answer");
      e.xStatus = "missing";
      throw e;
    }
    return { quotes, misses: missing.map((s) => [s, "missing"]) };
  };
}

export const STREAMS = ["quotes", "quotes-bridge"];
export async function pollQuotes({ root, config, untilMs, stream = "quotes", fetchImpl, now = Date.now, wait = sleep, log = console.log }) {
  if (!STREAMS.includes(stream)) throw new Error(`unknown stream ${stream}`);
  const qc = config.quotes ?? {};
  const everyMs = qc.everyMs ?? 5000;
  const snapshotEveryMs = qc.snapshotEveryMs ?? 600_000;
  const maxBytesPerSession = qc.maxBytesPerSession ?? 3_000_000;
  const maxFailures = qc.maxFailuresInRow ?? 6;
  const run = process.env.GITHUB_RUN_ID ?? "local";
  const state = readJson(join(root, "state", "state.json"), {});
  const symbols = new Set(quoteSymbols(state));
  const common = { run, untilMs, now, wait, log, maxFailures };

  const out = { symbols: symbols.size, polls: 0, failures: 0, rows: 0, sessions: 0, capped: false, stopped: null, venues: {} };
  const tasks = [];
  const guard = (target, p) =>
    p.then(
      (r) => Object.assign(target, r),
      (e) => {
        target.stopped = `error: ${e.message}`;
        log(`quotes task failed: ${e.stack ?? e.message}`);
      },
    );

  if (symbols.size === 0) out.stopped = "no symbols";
  else {
    const f = makeFetcher({ fetchImpl, spacingMs: config.spacingMs, now, wait, retries429: 0 });
    const poll = async () => {
      const { json: list } = await f.getJson(URL_ALL);
      return { quotes: list.filter((x) => symbols.has(x.symbol)).map((x) => [x.symbol, ...quote(x)]) };
    };
    const r = runTape({
      ...common, name: "bookTicker", dir: join(root, "data", "binanceus", stream), everyMs, snapshotEveryMs, maxBytesPerSession, poll, fatal: httpFatal,
    }).then(({ misses, ...rest }) => rest);
    tasks.push(guard(out, r));
  }

  const venue = (name, vc, poll, fatal) => {
    const vo = (out.venues[name] = { symbols: vc.n, polls: 0, failures: 0, misses: 0, rows: 0, sessions: 0, capped: false, stopped: null });
    const r = runTape({
      ...common,
      name,
      dir: join(root, "data", name, stream),
      everyMs: vc.everyMs ?? everyMs,
      snapshotEveryMs: vc.snapshotEveryMs ?? snapshotEveryMs,
      maxBytesPerSession: vc.maxBytesPerSession ?? 250_000,
      maxFailures: vc.maxFailuresInRow ?? maxFailures,
      poll,
      fatal,
    });
    tasks.push(guard(vo, r));
  };
  const fetcherFor = (vc, host) =>
    makeFetcher({
      fetchImpl,
      spacingMs: vc.spacingMs != null ? { ...config.spacingMs, [host]: vc.spacingMs } : config.spacingMs,
      timeoutMs: vc.timeoutMs ?? 5000,
      now,
      wait,
      retries429: 0,
    });

  const cbc = qc.venues?.coinbase;
  if (cbc?.products?.length) {
    const f = fetcherFor(cbc, "api.exchange.coinbase.com");
    venue("coinbase", { ...cbc, n: cbc.products.length }, coinbasePoll(f, cbc.products), httpFatal);
  }
  const krc = qc.venues?.kraken;
  const krPairs = Object.entries(krc?.pairs ?? {});
  if (krPairs.length) {
    // The key Kraken answers with: from the recorder's daily AssetPairs read when it has the pair, else config.
    const known = new Map(Object.values(state.kraken?.pairs ?? {}).map((p) => [p.altname, p.key]));
    const pairs = krPairs.map(([s, key]) => [s, known.get(s) ?? key ?? s]);
    const f = fetcherFor(krc, "api.kraken.com");
    venue("kraken", { ...krc, n: pairs.length }, krakenPoll(f, pairs), (e) => httpFatal(e) || e.rateLimited === true);
  }

  await Promise.all(tasks);
  return out;
}

function parseUntil(s) {
  if (!s) throw new Error("--until is required");
  const n = /^\d+$/.test(s) ? Number(s) : Date.parse(s);
  if (!Number.isFinite(n)) throw new Error(`bad --until ${s}`);
  return n;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const args = process.argv.slice(2);
  const root = resolve(args.find((a, i) => !a.startsWith("--") && args[i - 1] !== "--until" && args[i - 1] !== "--stream") ?? ".");
  const untilMs = parseUntil(args[args.indexOf("--until") + 1]);
  const stream = args.includes("--stream") ? args[args.indexOf("--stream") + 1] : "quotes";
  const config = JSON.parse(readFileSync(join(root, "recorder", "config.json"), "utf8"));
  const out = await pollQuotes({ root, config, untilMs, stream });
  console.log(JSON.stringify(out));
}
