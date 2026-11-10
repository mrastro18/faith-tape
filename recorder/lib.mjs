// Pure helpers for the recorder. No network and no clock here, so every function is testable.
import { createHash } from "node:crypto";
import { appendFileSync, existsSync, mkdirSync, readFileSync, writeFileSync, renameSync } from "node:fs";
import { dirname } from "node:path";

export const sha256 = (buf) => createHash("sha256").update(buf).digest("hex");

/** UTC day string for a millisecond timestamp. */
export const dayOf = (ms) => new Date(ms).toISOString().slice(0, 10);

/** Read JSON or return a fallback when the file is missing. A corrupt file throws: state must never be silently reset. */
export function readJson(path, fallback) {
  if (!existsSync(path)) return fallback;
  return JSON.parse(readFileSync(path, "utf8"));
}

/** Write JSON atomically (temp file then rename), sorted keys at the top level for stable diffs. */
export function writeJson(path, value) {
  mkdirSync(dirname(path), { recursive: true });
  const tmp = `${path}.tmp`;
  writeFileSync(tmp, JSON.stringify(value, null, 1) + "\n");
  renameSync(tmp, path);
}

/** Append rows as NDJSON lines, grouped into one file per UTC day. `rowDay` maps a row to its day. */
export function appendByDay(pathForDay, rows, rowDay) {
  const groups = new Map();
  for (const r of rows) {
    const d = rowDay(r);
    if (!groups.has(d)) groups.set(d, []);
    groups.get(d).push(JSON.stringify(r));
  }
  let written = 0;
  for (const [d, lines] of groups) {
    const p = pathForDay(d);
    mkdirSync(dirname(p), { recursive: true });
    appendFileSync(p, lines.join("\n") + "\n");
    written += lines.length;
  }
  return written;
}

/**
 * Aggregate trades into one-second bars. Trades: {t: ms, p: number, q: number}.
 * Only seconds strictly before `cutoffSec` are emitted, so a second still filling at fetch time is never split.
 * Bar row: [sec, open, high, low, close, volume, count, vwap].
 */
export function secondBars(trades, cutoffSec) {
  const bars = new Map();
  for (const tr of trades) {
    const s = Math.floor(tr.t / 1000);
    if (s >= cutoffSec) continue;
    let b = bars.get(s);
    if (!b) {
      b = { s, o: tr.p, h: tr.p, l: tr.p, c: tr.p, v: 0, n: 0, pv: 0, lastT: tr.t };
      bars.set(s, b);
    }
    if (tr.p > b.h) b.h = tr.p;
    if (tr.p < b.l) b.l = tr.p;
    if (tr.t >= b.lastT) {
      b.c = tr.p;
      b.lastT = tr.t;
    }
    b.v += tr.q;
    b.n += 1;
    b.pv += tr.p * tr.q;
  }
  return [...bars.values()]
    .sort((a, b) => a.s - b.s)
    .map((b) => [b.s, b.o, b.h, b.l, b.c, round(b.v, 10), b.n, b.v > 0 ? round(b.pv / b.v, 10) : b.c]);
}

export const round = (x, d) => {
  const f = 10 ** d;
  return Math.round(x * f) / f;
};

/** Sleep helper, injectable in tests. */
export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * A polite fetcher: fixed spacing between calls per host, a timeout, JSON parsing, and a record of every
 * non-200 status so the health file shows exactly what a venue answered.
 */
export function makeFetcher({ fetchImpl = fetch, spacingMs = {}, timeoutMs = 15000, now = Date.now, wait = sleep } = {}) {
  const last = new Map();
  const statuses = [];
  let calls = 0;
  async function getJson(url, { headers } = {}) {
    const host = new URL(url).host;
    const gap = spacingMs[host] ?? 120;
    const prev = last.get(host) ?? 0;
    const delay = prev + gap - now();
    if (delay > 0) await wait(delay);
    last.set(host, now());
    calls += 1;
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
      const res = await fetchImpl(url, { headers: { "user-agent": "faith-tape-recorder/1", ...headers }, signal: ctrl.signal });
      const text = await res.text();
      if (res.status !== 200) {
        statuses.push({ host, path: new URL(url).pathname, status: res.status, body: text.slice(0, 200) });
        const err = new Error(`HTTP ${res.status} ${host}${new URL(url).pathname}`);
        err.status = res.status;
        throw err;
      }
      return { json: JSON.parse(text), headers: res.headers };
    } finally {
      clearTimeout(timer);
    }
  }
  async function getText(url) {
    const host = new URL(url).host;
    calls += 1;
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
      const res = await fetchImpl(url, { headers: { "user-agent": "faith-tape-recorder/1" }, signal: ctrl.signal });
      return { status: res.status, text: await res.text(), host };
    } finally {
      clearTimeout(timer);
    }
  }
  return { getJson, getText, statuses, get calls() { return calls; } };
}

/** A deadline object so each task stops before the job's time budget runs out. */
export function makeDeadline(budgetMs, now = Date.now) {
  const end = now() + budgetMs;
  return { left: () => end - now(), passed: () => now() >= end };
}
