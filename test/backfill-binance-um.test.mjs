// Binance USD-M archive backfill tests against a fake data.binance.vision. No network.
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, existsSync } from "node:fs";
import { crc32, deflateRawSync, gunzipSync } from "node:zlib";
import { createHash } from "node:crypto";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { backfill, checkRange, parseFunding, parseKlines, parseMetrics, unzipSingle } from "../recorder/backfill-binance-um.mjs";

// A one-entry zip as Binance's archives are: local header, deflated data, optional data descriptor, central
// directory, end record. With `descriptor` the local header carries zero sizes, as streaming zippers write it.
function makeZip(name, text, { descriptor = false, stored = false } = {}) {
  const data = Buffer.from(text);
  const body = stored ? data : deflateRawSync(data);
  const crc = crc32(data);
  const n = Buffer.from(name);
  const local = Buffer.alloc(30);
  local.writeUInt32LE(0x04034b50, 0);
  local.writeUInt16LE(20, 4);
  local.writeUInt16LE(descriptor ? 8 : 0, 6);
  local.writeUInt16LE(stored ? 0 : 8, 8);
  local.writeUInt32LE(descriptor ? 0 : crc, 14);
  local.writeUInt32LE(descriptor ? 0 : body.length, 18);
  local.writeUInt32LE(descriptor ? 0 : data.length, 22);
  local.writeUInt16LE(n.length, 26);
  const dd = Buffer.alloc(descriptor ? 16 : 0);
  if (descriptor) {
    dd.writeUInt32LE(0x08074b50, 0);
    dd.writeUInt32LE(crc, 4);
    dd.writeUInt32LE(body.length, 8);
    dd.writeUInt32LE(data.length, 12);
  }
  const cd = Buffer.alloc(46);
  cd.writeUInt32LE(0x02014b50, 0);
  cd.writeUInt16LE(20, 4);
  cd.writeUInt16LE(20, 6);
  cd.writeUInt16LE(descriptor ? 8 : 0, 8);
  cd.writeUInt16LE(stored ? 0 : 8, 10);
  cd.writeUInt32LE(crc, 16);
  cd.writeUInt32LE(body.length, 20);
  cd.writeUInt32LE(data.length, 24);
  cd.writeUInt16LE(n.length, 28);
  cd.writeUInt32LE(0, 42);
  const cdOffset = local.length + n.length + body.length + dd.length;
  const end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50, 0);
  end.writeUInt16LE(1, 8);
  end.writeUInt16LE(1, 10);
  end.writeUInt32LE(cd.length + n.length, 12);
  end.writeUInt32LE(cdOffset, 16);
  return Buffer.concat([local, n, body, dd, cd, n, end]);
}

const sha = (b) => createHash("sha256").update(b).digest("hex");
const DAY = 86_400_000;
const KHEAD = "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore";
const MHEAD = "create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio";

function klinesCsv(ym, header) {
  const a = Date.UTC(Number(ym.slice(0, 4)), Number(ym.slice(5)) - 1, 1);
  const b = Date.UTC(Number(ym.slice(0, 4)), Number(ym.slice(5)), 1);
  const lines = [];
  for (let t = a; t < b; t += 300_000) lines.push(`${t},100.10,101.00,99.50,100.70,12.345,${t + 299_999},1234.5,42,6.1,610.2,0`);
  return (header ? KHEAD + "\n" : "") + lines.join("\n") + "\n";
}
function fundingCsv(ym) {
  const a = Date.UTC(Number(ym.slice(0, 4)), Number(ym.slice(5)) - 1, 1);
  return "calc_time,funding_interval_hours,last_funding_rate\n" + [0, 8, 16].map((h) => `${a + h * 3_600_000 + 3},8,0.00010000`).join("\n") + "\n";
}
function metricsCsv(day) {
  const lines = [];
  for (let i = 0; i < 288; i += 1) {
    const t = new Date(Date.parse(`${day}T00:00:00Z`) + i * 300_000).toISOString().replace("T", " ").slice(0, 19);
    lines.push(`${t},BTCUSDT,75832.13300000,3267094839.24937000,1.78965400,1.50000000,2.10000000,0.95000000`);
  }
  return MHEAD + "\n" + lines.join("\n") + "\n";
}

// Serves every archive in `have` (path suffix -> csv), its CHECKSUM, 404 for the rest; records every URL.
function fake({ csvFor, badSum = () => false, fail = () => false }) {
  const urls = [];
  const fetchImpl = async (url) => {
    urls.push(url);
    const u = new URL(url);
    const isSum = u.pathname.endsWith(".CHECKSUM");
    const zipPath = isSum ? u.pathname.slice(0, -9) : u.pathname;
    if (fail(zipPath)) return { status: 503, arrayBuffer: async () => new ArrayBuffer(0) };
    const text = csvFor(zipPath);
    if (text == null) return { status: 404, arrayBuffer: async () => new ArrayBuffer(0) };
    const zip = makeZip(zipPath.split("/").pop().replace(".zip", ".csv"), text, { descriptor: zipPath.includes("metrics") });
    const body = isSum ? Buffer.from(`${badSum(zipPath) ? sha(Buffer.from("x")) : sha(zip)}  ${zipPath.split("/").pop()}\n`) : zip;
    return { status: 200, arrayBuffer: async () => body.buffer.slice(body.byteOffset, body.byteOffset + body.length) };
  };
  return { fetchImpl, urls };
}

const read = (p) => gunzipSync(readFileSync(p)).toString().trim().split("\n").map((l) => JSON.parse(l));
const opts = { wait: async () => {}, log: () => {} };

test("unzipSingle reads deflated, stored and data-descriptor entries, and rejects a bad CRC", () => {
  const text = "a,b,c\n1,2,3\n".repeat(50);
  for (const o of [{}, { stored: true }, { descriptor: true }]) {
    const { name, data } = unzipSingle(makeZip("x.csv", text, o));
    assert.equal(name, "x.csv");
    assert.equal(data.toString(), text);
  }
  const z = makeZip("x.csv", text, { stored: true });
  z[40] ^= 1; // flip a byte of the stored data
  assert.throws(() => unzipSingle(z), /CRC/);
  assert.throws(() => unzipSingle(Buffer.alloc(40)), /end of central directory/);
});

test("parsers handle header and headerless files, microsecond times and empty ratios", () => {
  const withH = parseKlines(KHEAD + "\n1700000000000,1,2,0.5,1.5,10,1700000299999,15,7,4,6,0\n");
  const noH = parseKlines("1700000000000000,1,2,0.5,1.5,10,1700000299999999,15,7,4,6,0\n");
  assert.deepEqual(withH, [[1700000000000, 1, 2, 0.5, 1.5, 10, 15, 7, 4]]);
  assert.deepEqual(noH, withH);
  assert.deepEqual(parseFunding("calc_time,funding_interval_hours,last_funding_rate\n1704067200000,8,-0.00012\n"), [[1704067200000, -0.00012]]);
  const m = parseMetrics(MHEAD + "\n2021-12-01 00:05:00,BTCUSDT,1.5,2.5,,1.1,1.2,\n");
  assert.deepEqual(m, [[Date.UTC(2021, 11, 1, 0, 5), 1.5, 2.5, null, 1.1, 1.2, null]]);
  const q = parseMetrics(MHEAD + '\n2021-12-01 00:05:00,BTCUSDT,1.5,2.5,"",1.1,"1.2",""\n');
  assert.deepEqual(q, [[Date.UTC(2021, 11, 1, 0, 5), 1.5, 2.5, null, 1.1, 1.2, null]]);
  assert.throws(() => parseMetrics(MHEAD + "\n2021-12-01 00:05:00,BTCUSDT,abc,2.5,1,1,1,1\n"), /not a number/);
  assert.deepEqual(parseMetrics(MHEAD + "\n2021-12-01 00:05:00,BTCUSDT,1.5,2.5,1,1,1,1\n", "oi")[0], [Date.UTC(2021, 11, 1, 0, 5), 1.5, 2.5]);
});

test("refuses any month of the 2026 holdout", async () => {
  assert.throws(() => checkRange("2021-01", "2026-02"), /holdout/);
  assert.throws(() => checkRange("2026-01", "2026-02"), /holdout/);
  assert.throws(() => checkRange("2025-06", "2025-06"), /empty/);
  checkRange("2021-01", "2026-01");
  const w = fake({ csvFor: () => "" });
  await assert.rejects(backfill({ root: mkdtempSync(join(tmpdir(), "bum-")), syms: ["BTCUSDT"], from: "2025-12", to: "2026-03", fetchImpl: w.fetchImpl, ...opts }), /holdout/);
  assert.equal(w.urls.length, 0);
});

test("klines: one verified gzip file per month, header or not, missing months listed, and resumes", async () => {
  const root = mkdtempSync(join(tmpdir(), "bum-"));
  const w = fake({ csvFor: (p) => (p.includes("2021-01") ? null : p.includes("2021-02") ? klinesCsv("2021-02", true) : klinesCsv("2021-03", false)) });
  const out = await backfill({ root, syms: ["SOLUSDT"], kinds: ["klines"], from: "2021-01", to: "2021-04", fetchImpl: w.fetchImpl, ...opts });
  assert.equal(out.written, 2);
  assert.deepEqual(out.missing, { "SOLUSDT klines": ["2021-01"] });
  const dir = join(root, "data", "binance-um", "klines5m", "SOLUSDT");
  const feb = read(join(dir, "2021-02.ndjson.gz"));
  assert.equal(feb.length, 28 * 288);
  assert.deepEqual(feb[0], [Date.UTC(2021, 1, 1), 100.1, 101, 99.5, 100.7, 12.345, 1234.5, 42, 6.1]);
  assert.equal(read(join(dir, "2021-03.ndjson.gz")).length, 31 * 288);
  assert.ok(w.urls.every((u) => u.startsWith("https://data.binance.vision/data/futures/um/monthly/klines/SOLUSDT/5m/")));
  assert.ok(w.urls.every((u) => !u.includes("2026")));
  assert.deepEqual(JSON.parse(readFileSync(join(dir, "missing.json"), "utf8")).missing, ["2021-01"]);
  const again = await backfill({ root, syms: ["SOLUSDT"], kinds: ["klines"], from: "2021-02", to: "2021-04", fetchImpl: w.fetchImpl, ...opts });
  assert.deepEqual([again.written, again.skipped], [0, 2]);
});

test("a bad checksum or a failed fetch writes nothing for that month and is reported", async () => {
  const root = mkdtempSync(join(tmpdir(), "bum-"));
  const w = fake({ csvFor: (p) => klinesCsv(p.match(/(\d{4}-\d{2})\.zip/)[1], true), badSum: (p) => p.includes("2021-02"), fail: (p) => p.includes("2021-03") });
  const out = await backfill({ root, syms: ["ETHUSDT"], kinds: ["klines"], from: "2021-02", to: "2021-04", fetchImpl: w.fetchImpl, ...opts });
  assert.equal(out.written, 0);
  assert.equal(out.failed.length, 2);
  assert.match(out.failed[0], /checksum mismatch/);
  assert.match(out.failed[1], /HTTP 503/);
  assert.ok(!existsSync(join(root, "data", "binance-um", "klines5m", "ETHUSDT", "2021-02.ndjson.gz")));
});

test("funding: one file per full year; a partial range writes nothing", async () => {
  const root = mkdtempSync(join(tmpdir(), "bum-"));
  const w = fake({ csvFor: (p) => fundingCsv(p.match(/(\d{4}-\d{2})\.zip/)[1]) });
  const out = await backfill({ root, syms: ["DOGEUSDT"], kinds: ["funding"], from: "2024-01", to: "2025-06", fetchImpl: w.fetchImpl, ...opts });
  assert.equal(out.written, 1);
  const rows = read(join(root, "data", "binance-um", "funding", "DOGEUSDT", "2024.ndjson.gz"));
  assert.equal(rows.length, 36);
  assert.deepEqual(rows[0], [Date.UTC(2024, 0, 1) + 3, 0.0001]);
  assert.ok(!existsSync(join(root, "data", "binance-um", "funding", "DOGEUSDT", "2025.ndjson.gz")));
});

test("metrics: daily files joined per month, missing days and months listed, ratio columns optional", async () => {
  const root = mkdtempSync(join(tmpdir(), "bum-"));
  const csvFor = (p) => {
    const d = p.match(/(\d{4}-\d{2}-\d{2})\.zip/)[1];
    if (d.startsWith("2021-01") || d === "2021-02-03") return null;
    return metricsCsv(d);
  };
  const w = fake({ csvFor });
  const out = await backfill({ root, syms: ["BTCUSDT"], kinds: ["metrics"], from: "2021-01", to: "2021-03", fetchImpl: w.fetchImpl, concurrency: 3, ...opts });
  assert.equal(out.written, 1);
  assert.deepEqual(out.missing["BTCUSDT metrics"], ["2021-01", "2021-02-03"]);
  const feb = read(join(root, "data", "binance-um", "metrics", "BTCUSDT", "2021-02.ndjson.gz"));
  assert.equal(feb.length, 27 * 288);
  assert.deepEqual(feb[0], [Date.UTC(2021, 1, 1), 75832.133, 3267094839.24937, 1.789654, 1.5, 2.1, 0.95]);
  assert.ok(feb.every((r, i) => i === 0 || r[0] > feb[i - 1][0]));

  const root3 = mkdtempSync(join(tmpdir(), "bum-"));
  const w3 = fake({ csvFor: (p) => (p.includes("2021-02-05") ? MHEAD + "\n2021-02-05 00:00:00,BTCUSDT,x,1,1,1,1,1\n" : csvFor(p)) });
  const bad = await backfill({ root: root3, syms: ["BTCUSDT"], kinds: ["metrics"], from: "2021-02", to: "2021-03", fetchImpl: w3.fetchImpl, ...opts });
  assert.equal(bad.written, 0);
  assert.match(bad.failed[0], /2021-02-05: not a number/);

  const root2 = mkdtempSync(join(tmpdir(), "bum-"));
  const w2 = fake({ csvFor });
  await backfill({ root: root2, syms: ["BTCUSDT"], kinds: ["metrics"], from: "2021-01", to: "2021-03", metricsFrom: "2021-02", metricsCols: "oi", fetchImpl: w2.fetchImpl, ...opts });
  assert.ok(w2.urls.every((u) => !u.includes("2021-01-")));
  assert.equal(read(join(root2, "data", "binance-um", "metrics", "BTCUSDT", "2021-02.ndjson.gz"))[0].length, 3);
});
