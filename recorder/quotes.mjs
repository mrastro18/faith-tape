#!/usr/bin/env node
// Binance.US best bid and ask for the recorded pairs, polled while the chain waits for its next run.
// Usage: node recorder/quotes.mjs [rootDir] --until <ISO time or epoch ms>
// One call to /api/v3/ticker/bookTicker returns every pair, so a poll costs the same for 1 pair or 260.
// Rows, in data/binanceus/quotes/<day>.ndjson:
//   ["s", ms, run, everyMs]                        a polling session starts
//   ["f", ms, symbol, bid, bidQty, ask, askQty]    a full snapshot of every recorded pair (at the start,
//                                                  then every `snapshotEveryMs`)
//   ["q", ms, symbol, bid, bidQty, ask, askQty]    the bid or ask price changed since the last poll
//   ["e", ms, polls, failures, capped]             the session ends
// ms is when the answer arrived. Between an "s" and its "e" every change of best price is here at poll
// resolution: a price that came and went between two polls is not. A size change alone is not a row.
// Outside sessions nothing is known. A session that crosses midnight ends and restarts at the boundary.
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { readFileSync, existsSync, statSync } from "node:fs";
import { appendByDay, dayOf, makeFetcher, readJson, sleep } from "./lib.mjs";

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

export async function pollQuotes({ root, config, untilMs, fetchImpl, now = Date.now, wait = sleep, log = console.log }) {
  const qc = config.quotes ?? {};
  const everyMs = qc.everyMs ?? 5000;
  const snapshotEveryMs = qc.snapshotEveryMs ?? 600_000;
  const maxBytesPerDay = qc.maxBytesPerDay ?? 60e6;
  const maxFailures = qc.maxFailuresInRow ?? 6;
  const run = process.env.GITHUB_RUN_ID ?? "local";
  const state = readJson(join(root, "state", "state.json"), {});
  const symbols = new Set(quoteSymbols(state));
  const f = makeFetcher({ fetchImpl, spacingMs: config.spacingMs, now, wait, retries429: 0 });
  const file = (d) => join(root, "data", "binanceus", "quotes", `${d}.ndjson`);
  const sizeOf = (d) => (existsSync(file(d)) ? statSync(file(d)).size : 0);

  let rows = [];
  let bytes = 0; // bytes buffered for the current day since the last flush
  const flush = () => {
    if (rows.length) appendByDay(file, rows, (r) => dayOf(r[1]));
    rows = [];
  };
  const push = (r) => {
    rows.push(r);
    bytes += JSON.stringify(r).length + 1;
  };

  const out = { symbols: symbols.size, polls: 0, failures: 0, rows: 0, sessions: 0, capped: false, stopped: null };
  if (symbols.size === 0) {
    out.stopped = "no symbols";
    return out;
  }
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
    bytes = sizeOf(day);
    capped = bytes > maxBytesPerDay;
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
    nextAt += everyMs;
    let list;
    try {
      ({ json: list } = await f.getJson(URL_ALL));
      inRow = 0;
    } catch (e) {
      out.failures += 1;
      if (day) failures += 1;
      inRow += 1;
      log(`bookTicker failed: ${e.message}`);
      if (e.status === 429 || e.status === 418 || e.status === 403 || inRow >= maxFailures) {
        out.stopped = e.status ? `HTTP ${e.status}` : "failures";
        break;
      }
      continue;
    }
    const t = now();
    if (day && dayOf(t) !== day) {
      close(lastT ?? t);
      flush();
    }
    if (!day || dayOf(t) !== day) open(t);
    polls += 1;
    out.polls += 1;
    lastT = t;
    const full = t - lastSnap >= snapshotEveryMs;
    if (full) lastSnap = t;
    if (!capped && bytes > maxBytesPerDay) capped = true;
    for (const x of list) {
      if (!symbols.has(x.symbol)) continue;
      const q = quote(x);
      const p = last.get(x.symbol);
      last.set(x.symbol, q);
      if (full) push(["f", t, x.symbol, ...q]);
      else if (!capped && (!p || p[0] !== q[0] || p[2] !== q[2])) push(["q", t, x.symbol, ...q]);
    }
    if (now() - lastFlush >= 60_000) {
      out.rows += rows.length;
      flush();
      lastFlush = now();
    }
  }
  if (day) close(lastT ?? now());
  out.rows += rows.length;
  flush();
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
  const root = resolve(args.find((a, i) => !a.startsWith("--") && args[i - 1] !== "--until") ?? ".");
  const untilMs = parseUntil(args[args.indexOf("--until") + 1]);
  const config = JSON.parse(readFileSync(join(root, "recorder", "config.json"), "utf8"));
  const out = await pollQuotes({ root, config, untilMs });
  console.log(JSON.stringify(out));
}
