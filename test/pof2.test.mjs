// P-OF2 universe rule and coverage tests. No network.
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { gzipSync } from "node:zlib";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { coverage, firstMonthFromKeys, refuseHoldout, stepA, stepB, stepC, stepD } from "../recorder/pof2.mjs";

test("refuses the 2026 holdout", () => {
  assert.throws(() => refuseHoldout("2022-01", "2026-02"), /holdout/);
  assert.throws(() => refuseHoldout("2026-01", "2026-03"), /holdout/);
  refuseHoldout("2022-01", "2026-01");
});

test("step a keeps TRADING USD/USDT bases and drops stables, wrapped, pegged and used ones", () => {
  const s = (base, quote, status = "TRADING") => ({ symbol: base + quote, baseAsset: base, quoteAsset: quote, status });
  const a = stepA({ symbols: [s("ADA", "USD"), s("ADA", "USDT"), s("LINK", "USDC"), s("AVAX", "USDT", "BREAK"), s("USDC", "USD"), s("WBTC", "USD"), s("PAXG", "USDT"), s("BTC", "USD"), s("EUR", "USDT"), s("SHIB", "USDT")] });
  assert.deepEqual(a.kept, ["ADA", "SHIB"]);
  assert.deepEqual(a.candidates.find((c) => c.base === "ADA").pairs, ["ADAUSD", "ADAUSDT"]);
  assert.deepEqual(a.dropped.map((d) => d.base), ["BTC", "EUR", "PAXG", "USDC", "WBTC"]);
  assert.ok(!a.candidates.some((c) => c.base === "LINK" || c.base === "AVAX"));
});

test("step b needs an online product with candles on both probe days", async () => {
  const products = [
    { id: "ADA-USD", status: "online", trading_disabled: false },
    { id: "NEW-USD", status: "online", trading_disabled: false },
    { id: "OFF-USD", status: "delisted", trading_disabled: true },
  ];
  const b = await stepB(["ADA", "NEW", "OFF", "NONE"], products, async (id, day) => (id === "NEW-USD" && day === "2022-01-01" ? 0 : 288));
  assert.deepEqual(b.kept, ["ADA"]);
  assert.deepEqual(b.dropped.map((d) => d.base), ["NEW", "OFF", "NONE"]);
  assert.deepEqual(b.probes.ADA, { product: "ADA-USD", candles20220101: 288, candles20251201: 288 });
});

test("step c finds the archive's symbol, including 1000-prefixed ones", async () => {
  const have = new Set(["ADAUSDT-2022-01-01", "ADAUSDT-2025-12-31", "1000SHIBUSDT-2022-01-01", "1000SHIBUSDT-2025-12-31", "LATEUSDT-2025-12-31"]);
  const exists = async (url) => have.has(/metrics\/([^/]+)\/\1-metrics-(\d{4}-\d{2}-\d{2})\.zip$/.exec(url).slice(1).join("-"));
  const c = await stepC(["ADA", "SHIB", "LATE"], exists);
  assert.deepEqual(c.kept, [{ base: "ADA", perp: "ADAUSDT", renamed: false }, { base: "SHIB", perp: "1000SHIBUSDT", renamed: true }]);
  assert.match(c.dropped[0].why, /LATEUSDT has 2025-12-31 only/);
});

test("step d orders by first klines month then base and keeps 15", async () => {
  const kept = Array.from({ length: 17 }, (_, i) => ({ base: `C${String(i).padStart(2, "0")}`, perp: `C${String(i).padStart(2, "0")}USDT` }));
  const first = async (sym) => ({ firstKlinesMonth: sym === "C16USDT" ? "2019-12" : sym < "C05" ? "2021-01" : "2020-06", method: "listing" });
  const d = await stepD(kept, first);
  assert.equal(d.applied, true);
  assert.equal(d.kept.length, 15);
  assert.equal(d.kept[0].base, "C16");
  assert.equal(d.kept[1].base, "C05");
  assert.deepEqual(d.cut.map((c) => c.base), ["C03", "C04"]);
  assert.equal(firstMonthFromKeys(["p/X-5m-2021-03.zip", "p/X-5m-2020-11.zip", "p/X-5m-2020-11.zip.CHECKSUM"], "X"), "2020-11");
});

test("coverage counts months, five-minute slots and hour pairs from timestamps only", () => {
  const root = mkdtempSync(join(tmpdir(), "pof2-"));
  const cb = join(root, "data", "coinbase", "candles5m", "ADA");
  const me = join(root, "data", "binance-um", "metrics", "ADAUSDT");
  mkdirSync(cb, { recursive: true });
  mkdirSync(me, { recursive: true });
  const t0 = Date.UTC(2022, 0, 1);
  const gz = (rows) => gzipSync(rows.map((r) => JSON.stringify(r)).join("\n") + "\n");
  writeFileSync(join(cb, "2022-01.ndjson.gz"), gz([[t0, 1, 1, 1, 1, 1], [t0 + 300_000, 1, 1, 1, 1, 1]]));
  writeFileSync(join(me, "2022-01.ndjson.gz"), gz([[t0, 5], [t0 + 3_600_000, 5], [t0 + 7_200_000, 5], [t0 + 10_800_001, 5]]));
  writeFileSync(join(me, "missing.json"), JSON.stringify({ missing: ["2022-02-03"] }));
  const c = coverage(root, "ADA", "ADAUSDT");
  assert.equal(c.coinbase.months, 1);
  assert.equal(c.coinbase.missingMonths.length, 47);
  assert.equal(c.coinbase.candles, 2);
  assert.equal(c.coinbase.slots, 1461 * 288);
  assert.equal(c.metrics.hours, 1461 * 24 - 1);
  assert.equal(c.metrics.hoursWithTandTminus1h, 2);
  assert.equal(c.metrics.offGrid, 1);
  assert.deepEqual(c.metrics.archiveMissing, ["2022-02-03"]);
  assert.equal(c.coinbase.daysWithoutRow, 1460);
  assert.equal(c.coinbase.longestGapFrom, "2022-01-01T00:10Z");
  assert.equal(c.coinbase.longestGapMinutes, 1461 * 1440 - 10);
});
