#!/usr/bin/env node
// Feed-order diagnostic for tick.mjs's 250 ms tolerance (exploratory, after queue-prov-r2's results): how
// far the book leads or lags the prints in Tardis's local timestamps. No fill or markout is computed.
//   node tools/maker-sim/lead.mjs --quotes q.csv.gz --trades t.csv.gz --day YYYY-MM-DD
// For a print against the last book row strictly before it:
//   - "inside" (its price is past its side's touch, toward the other side: the book already moved away):
//     book lead = print time - time of the first row of the run of rows, ending at that last row, whose
//     touch on that side is past the print price. Large leads would defeat the tolerance.
//   - "through" (its price is beyond its side's touch: the print came before the book showed it):
//     print lead = time of the first later row whose touch on that side is at or beyond the print price.
// Leads are capped at 5 s; the share beyond is reported.
import { loadQuotes, loadTrades, TICK } from "./tick.mjs";
import { coverAt, rowAt } from "./queue.mjs";
import { dayAllowed } from "./run.mjs";

const CAP = 5000;
const EDGES = [10, 50, 100, 250, 500, 1000, 2000, 5000];

export function leads(q, s, tr, tick) {
  const tk = (p) => Math.round(p / tick);
  const out = { inside: [], through: [], atTouch: 0, n: 0 };
  for (let j = 0; j < tr.n; j++) {
    const t = tr.t[j];
    const iv = coverAt(q.intervals, t);
    if (!iv) continue;
    const i = rowAt(s, t - 1e-6, iv.session);
    if (i < 0 || !(s.bid[i] > 0 && s.ask[i] > 0)) continue;
    out.n++;
    const bidSide = tr.bm[j] === 1; // a seller took: the print is on the bid side
    const xp = tk(tr.p[j]);
    const touch = (k) => tk(bidSide ? s.bid[k] : s.ask[k]);
    const past = (k) => (bidSide ? touch(k) < xp : touch(k) > xp); // book already away from the print
    const beyond = (k) => (bidSide ? touch(k) > xp : touch(k) < xp); // print beyond the touch
    if (touch(i) === xp) out.atTouch++;
    else if (past(i)) {
      let k = i;
      while (k - 1 >= 0 && s.s[k - 1] === iv.session && past(k - 1) && t - s.t[k - 1] <= CAP) k--;
      out.inside.push(Math.min(CAP, t - s.t[k]));
    } else if (beyond(i)) {
      let k = i + 1;
      while (k < s.t.length && s.s[k] === iv.session && beyond(k) && s.t[k] - t <= CAP) k++;
      out.through.push(k < s.t.length ? Math.min(CAP, s.t[k] - t) : CAP);
    }
  }
  return out;
}

const hist = (xs) => {
  const a = [...xs].sort((x, y) => x - y);
  const q = (p) => (a.length ? a[Math.min(a.length - 1, Math.floor(p * a.length))] : null);
  return {
    n: a.length,
    p50: q(0.5),
    p90: q(0.9),
    p99: q(0.99),
    atMost: Object.fromEntries(EDGES.map((e) => [e, a.length ? a.filter((x) => x <= e).length / a.length : null])),
  };
};

async function main() {
  const arg = (k) => process.argv[process.argv.indexOf(k) + 1];
  const day = arg("--day");
  if (!dayAllowed(day)) process.exit(4);
  const Q = await loadQuotes(arg("--quotes"), TICK);
  const tr = await loadTrades(arg("--trades"));
  const r = leads(Q.q, Q.s, tr, Q.tick);
  console.log(
    JSON.stringify({
      day,
      prints: r.n,
      atTouch: r.atTouch,
      insideBookLeadMs: hist(r.inside),
      throughPrintLeadMs: hist(r.through),
    }),
  );
}

import { fileURLToPath } from "node:url";
import { resolve } from "node:path";
if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) main();
