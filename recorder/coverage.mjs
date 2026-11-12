#!/usr/bin/env node
// Quote coverage from metadata only (CHANGES 2026-11-12d): which minutes of each UTC day have a quote session,
// per venue, read from file names alone (the manifests for offloaded days, the checkout's directory listing for
// older ones). No quote file is opened, so it can run on days that are sealed for reading (queue-prov-r3 reads
// quotes from 2026-11-12 only once 30 days exist).
//
// A session file is named <HHMMSS>Z-<run>.ndjson.gz by the time it started. A session is taken to run until
// the next session's start, at most SESSION_MIN minutes. That is an estimate, not the poll count queue-prov-r3
// uses (it counts minutes with a poll), so it is an early warning: a day under 720 estimated minutes will not
// count for r3, and a day over it usually will.
//
//   node recorder/coverage.mjs [root] [--from YYYY-MM-DD] [--to YYYY-MM-DD] [--venue binanceus] [--json]
import { existsSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { manifestLines } from "./manifest.mjs";

export const SESSION_MIN = 31;
export const DAY_MIN = 720;
const NAME = /^(\d{2})(\d{2})(\d{2})Z-[^/]+\.ndjson(\.gz)?$/;

/** Session starts as minutes after midnight, per "venue|day", from manifests and from files in the checkout. */
export function sessionStarts(root, { from, to } = {}) {
  const out = new Map();
  const add = (venue, day, name) => {
    const m = NAME.exec(name);
    if (!m || (from && day < from) || (to && day > to)) return;
    const key = `${venue}|${day}`;
    if (!out.has(key)) out.set(key, new Set());
    out.get(key).add(Number(m[1]) * 60 + Number(m[2]) + Number(m[3]) / 60);
  };
  for (const line of manifestLines(root, { from, to }))
    for (const f of line.files ?? []) {
      const p = f.path.split("/");
      if (p[0] === "data" && p[2] === "quotes" && p.length === 5) add(p[1], p[3], p[4]);
    }
  const data = join(root, "data");
  if (existsSync(data))
    for (const venue of readdirSync(data)) {
      const q = join(data, venue, "quotes");
      if (!existsSync(q)) continue;
      for (const day of readdirSync(q)) {
        if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) continue;
        for (const name of readdirSync(join(q, day))) add(venue, day, name);
      }
    }
  return out;
}

/** Estimated covered minutes and the longest uncovered stretch of one day, from its session starts. */
export function dayCoverage(starts, { endMin = 1440 } = {}) {
  const s = [...starts].sort((a, b) => a - b);
  let covered = 0;
  let longestGap = s.length ? s[0] : endMin;
  for (let i = 0; i < s.length; i++) {
    const next = i + 1 < s.length ? s[i + 1] : endMin;
    const run = Math.min(SESSION_MIN, Math.max(0, next - s[i]));
    covered += Math.min(run, Math.max(0, endMin - s[i]));
    longestGap = Math.max(longestGap, next - s[i] - run);
  }
  return { sessions: s.length, minutes: Math.round(covered), longestGapMin: Math.round(longestGap) };
}

export function coverage(root, { from, to, venue, now = new Date() } = {}) {
  const today = now.toISOString().slice(0, 10);
  const nowMin = now.getUTCHours() * 60 + now.getUTCMinutes();
  const rows = [];
  for (const [key, starts] of sessionStarts(root, { from, to })) {
    const [v, day] = key.split("|");
    if (venue && v !== venue) continue;
    const partial = day === today;
    const c = dayCoverage(starts, { endMin: partial ? nowMin : 1440 });
    rows.push({ venue: v, day, partial, ...c, counts: !partial && c.minutes >= DAY_MIN });
  }
  rows.sort((a, b) => (a.venue === b.venue ? (a.day < b.day ? -1 : 1) : a.venue < b.venue ? -1 : 1));
  return rows;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const args = process.argv.slice(2);
  const opt = (k) => {
    const i = args.indexOf(k);
    return i >= 0 ? args[i + 1] : undefined;
  };
  const root = args[0] && !args[0].startsWith("--") ? args[0] : ".";
  const rows = coverage(root, { from: opt("--from"), to: opt("--to"), venue: opt("--venue") });
  if (args.includes("--json")) console.log(JSON.stringify(rows, null, 2));
  else {
    for (const r of rows)
      console.log(
        `${r.venue.padEnd(10)} ${r.day}${r.partial ? " (so far)" : ""}: ${String(r.sessions).padStart(2)} sessions, ~${r.minutes} min, longest gap ${r.longestGapMin} min${r.partial ? "" : r.counts ? "" : "  BELOW 720"}`,
      );
    const bad = rows.filter((r) => !r.partial && !r.counts);
    if (bad.length) console.log(`::warning::${bad.length} venue-days under ${DAY_MIN} estimated minutes`);
  }
}
