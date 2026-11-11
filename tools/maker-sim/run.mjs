#!/usr/bin/env node
// One pair, one UTC day of Tardis.dev tick data: simulate every placement (tick.mjs runDay) and write the
// compact rows the aggregator reads. Raw files are read from the runner's temp dir and never committed.
//   node tools/maker-sim/run.mjs --venue coinbase --symbol BTC-USD --day 2021-01-01 \
//     --quotes q.csv.gz --trades t.csv.gz --out out/coinbase-BTC-USD-2021-01-01.json.gz [--structure]
// --structure prints only the shape of the run (rows, tick, coverage, run time, that every placement gave
// a row) and writes nothing: it is for checking the pipeline before a spec is registered, and shows no
// fill, rate or markout figure.
import { createHash } from "node:crypto";
import { createReadStream, mkdirSync, statSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { gzipSync } from "node:zlib";
import { ORDER_FIELDS, CONTROL_FIELDS, STATUS, TICK, loadQuotes, loadTrades, runDay } from "./tick.mjs";

const arg = (k) => {
  const i = process.argv.indexOf(k);
  return i > 0 ? process.argv[i + 1] : undefined;
};
const has = (k) => process.argv.includes(k);

/** Days FAITH reserves: 2026 is the sealed holdout; 2024 and 2025 are left for a separate test. */
export function dayAllowed(day) {
  const y = Number(day.slice(0, 4));
  return /^\d{4}-\d{2}-\d{2}$/.test(day) && y >= 2019 && y <= 2023;
}

const sha256 = (path) =>
  new Promise((res, rej) => {
    const h = createHash("sha256");
    createReadStream(path)
      .on("data", (d) => h.update(d))
      .on("end", () => res(h.digest("hex")))
      .on("error", rej);
  });

async function main() {
  const venue = arg("--venue");
  const symbol = arg("--symbol");
  const day = arg("--day");
  const qPath = arg("--quotes");
  const tPath = arg("--trades");
  const outPath = arg("--out");
  if (!venue || !symbol || !day || !qPath || !tPath || (!outPath && !has("--structure"))) {
    console.error("usage: run.mjs --venue v --symbol s --day YYYY-MM-DD --quotes f --trades f --out f [--structure]");
    process.exit(2);
  }
  if (!dayAllowed(day)) {
    console.error(`refused: ${day} is outside 2019-2023 (2026 is FAITH's holdout; 2024-2025 are reserved)`);
    process.exit(4);
  }
  const t0 = Date.now();
  const files = {
    quotes: { bytes: statSync(qPath).size, sha256: await sha256(qPath) },
    trades: { bytes: statSync(tPath).size, sha256: await sha256(tPath) },
  };
  const Q = await loadQuotes(qPath, TICK);
  const tr = await loadTrades(tPath);
  const tLoad = Date.now();
  const dayStartMs = Date.parse(`${day}T00:00:00Z`);
  const res = runDay({ q: Q.q, s: Q.s, tr, tick: Q.tick, dayStartMs, cfg: TICK });
  const tSim = Date.now();
  const coverage = {
    stretches: Q.q.intervals.length,
    coveredMin: Q.q.intervals.reduce((a, iv) => a + (iv.b - iv.a), 0) / 60_000,
    first: Q.s.t.length ? new Date(Q.s.t[0]).toISOString() : null,
    last: Q.s.t.length ? new Date(Q.s.t.at(-1)).toISOString() : null,
  };
  const expectRows = res.placements * TICK.sides.length * TICK.variants.length * TICK.cancels.length;
  const shape = {
    venue,
    symbol,
    day,
    files,
    quotes: Q.stat,
    trades: tr.stat,
    tick: Q.tick,
    coverage,
    placements: res.placements,
    noBook: res.noBook,
    orderRows: res.orders.length,
    rowsMatchPlacements: res.orders.length === expectRows,
    badStatus: res.orders.filter((r) => !(r[3] >= 0 && r[3] < STATUS.length)).length,
    seconds: { load: (tLoad - t0) / 1000, simulate: (tSim - tLoad) / 1000 },
  };
  if (has("--structure")) {
    console.log(JSON.stringify(shape));
    return;
  }
  const out = {
    schema: "faith-tape.maker-sim.day/1",
    ...shape,
    config: TICK,
    orderFields: ORDER_FIELDS,
    controlFields: CONTROL_FIELDS,
    calibration: res.calibration,
    orders: res.orders,
    control: res.control,
  };
  mkdirSync(dirname(outPath), { recursive: true });
  writeFileSync(outPath, gzipSync(JSON.stringify(out)));
  console.log(JSON.stringify(shape));
}

const isMain = process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1]);
if (isMain) main().catch((e) => {
  console.error(e);
  process.exit(1);
});
