// Offload and materialize on a temporary git repository with a fake release store. No network.
import { test } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { appendFileSync, copyFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { offload, pending, dayOfPath } from "../recorder/offload.mjs";
import { check, materialize, manifestLines } from "../recorder/materialize.mjs";

const git = (root, ...a) => execFileSync("git", ["-C", root, ...a], { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] });
const put = (root, p, s) => {
  mkdirSync(dirname(join(root, p)), { recursive: true });
  writeFileSync(join(root, p), s);
};

function repo() {
  const root = mkdtempSync(join(tmpdir(), "tape-"));
  git(root, "init", "-q", "-b", "main");
  git(root, "config", "user.email", "t@example.invalid");
  git(root, "config", "user.name", "t");
  put(root, "README.md", "x\n");
  put(root, "data/coinbase/bars1s/2026-11-11/BTC.ndjson", "[1,2]\n[2,3]\n");
  git(root, "add", ".");
  git(root, "commit", "-q", "-m", "init");
  return root;
}

// a fake release store: tag -> name -> bytes
function store({ failUpload = false, wrongSize = false } = {}) {
  const S = new Map();
  return {
    S,
    ensureRelease(tag) {
      if (!S.has(tag)) S.set(tag, new Map());
    },
    upload(tag, file) {
      if (failUpload) throw new Error("upload refused");
      S.get(tag).set(file.split("/").pop(), readFileSync(file));
    },
    assets(tag) {
      return [...(S.get(tag) ?? new Map())].map(([name, b]) => ({ name, size: b.length + (wrongSize ? 1 : 0) }));
    },
    download(tag, name, dir) {
      const p = join(dir, name);
      writeFileSync(p, S.get(tag).get(name));
      return p;
    },
    releases() {
      return [...S.keys()];
    },
  };
}
const quiet = () => {};
const commitAll = (root, msg) => {
  git(root, "add", "-A");
  git(root, "commit", "-q", "-m", msg);
};

test("dayOfPath reads the day directory", () => {
  assert.equal(dayOfPath("data/binanceus/quotes/2026-11-12/s1.ndjson.gz"), "2026-11-12");
  assert.equal(dayOfPath("data/binanceus/quotes/x.gz"), null);
});

test("quotes: untracked session files leave git as one bundle per day and come back byte for byte", () => {
  const root = repo();
  const a = Buffer.from([0x1f, 0x8b, 1, 2, 3]);
  const b = Buffer.from("raw session\n");
  put(root, "data/binanceus/quotes/2026-11-12/s1.ndjson.gz", a);
  put(root, "data/kraken/quotes/2026-11-12/s1.ndjson.gz", a);
  put(root, "data/coinbase/quotes/2026-11-13/s2.ndjson", b);
  const client = store();
  const r = offload(root, { kind: "quotes", run: "77", client, log: quiet });
  assert.equal(r.bundles.length, 2);
  assert.deepEqual(r.kept, []);
  assert.equal(existsSync(join(root, "data/binanceus/quotes/2026-11-12/s1.ndjson.gz")), false);
  assert.equal(client.S.get("tape-2026-11-12").size, 1);
  assert.deepEqual(manifestLines(root).map((m) => m.asset), ["quotes-2026-11-12-77.tar.gz", "quotes-2026-11-13-77.tar.gz"]);
  assert.equal(git(root, "status", "--porcelain", "--", "data").trim(), "", "nothing left under data");
  commitAll(root, "quotes");
  const m = materialize(root, { client, log: quiet });
  assert.equal(m.applied, 2);
  assert.deepEqual(readFileSync(join(root, "data/kraken/quotes/2026-11-12/s1.ndjson.gz")), a);
  assert.deepEqual(readFileSync(join(root, "data/coinbase/quotes/2026-11-13/s2.ndjson")), b);
  assert.equal(materialize(root, { client, log: quiet }).applied, 0, "a second run applies nothing");
  rmSync(root, { recursive: true, force: true });
});

test("bars: a tracked day file gives up only its appended tail, and the chunks rebuild the whole day", () => {
  const root = repo();
  const client = store();
  const day11 = "data/coinbase/bars1s/2026-11-11/BTC.ndjson";
  const day12 = "data/coinbase/bars1s/2026-11-12/BTC.ndjson";
  // run 1: appends to the tracked 11th and starts the 12th
  appendFileSync(join(root, day11), "[3,4]\n");
  put(root, day12, "[10,1]\n");
  const p = pending(root, "bars");
  assert.deepEqual(p.map((x) => [x.path, x.mode, x.bytes.toString()]).sort(), [
    [day11, "append", "[3,4]\n"],
    [day12, "append", "[10,1]\n"],
  ]);
  const r1 = offload(root, { kind: "bars", run: "1", client, log: quiet });
  assert.equal(r1.bundles.length, 2);
  assert.equal(readFileSync(join(root, day11), "utf8"), "[1,2]\n[2,3]\n", "restored to HEAD");
  assert.equal(existsSync(join(root, day12)), false);
  commitAll(root, "run 1");
  // run 2 on a fresh checkout: the 12th's file starts empty and gets only new rows
  put(root, day12, "[11,2]\n[12,3]\n");
  offload(root, { kind: "bars", run: "2", client, log: quiet });
  commitAll(root, "run 2");
  assert.equal(git(root, "ls-files", "data/coinbase/bars1s/2026-11-12").trim(), "");
  materialize(root, { client, log: quiet });
  assert.equal(readFileSync(join(root, day11), "utf8"), "[1,2]\n[2,3]\n[3,4]\n");
  assert.equal(readFileSync(join(root, day12), "utf8"), "[10,1]\n[11,2]\n[12,3]\n");
  rmSync(root, { recursive: true, force: true });
});

test("bars: a tracked file that changed other than by appending is left for git", () => {
  const root = repo();
  writeFileSync(join(root, "data/coinbase/bars1s/2026-11-11/BTC.ndjson"), "[9,9]\n[2,3]\n[3,4]\n");
  assert.deepEqual(pending(root, "bars"), []);
  rmSync(root, { recursive: true, force: true });
});

test("a failed upload or an asset missing at its size keeps every file for git and writes no manifest", () => {
  for (const opt of [{ failUpload: true }, { wrongSize: true }]) {
    const root = repo();
    put(root, "data/binanceus/quotes/2026-11-12/s1.ndjson.gz", "q");
    appendFileSync(join(root, "data/coinbase/bars1s/2026-11-11/BTC.ndjson"), "[3,4]\n");
    const client = store(opt);
    const q = offload(root, { kind: "quotes", run: "5", client, log: quiet });
    const b = offload(root, { kind: "bars", run: "5", client, log: quiet });
    assert.equal(q.bundles.length + b.bundles.length, 0);
    assert.deepEqual(q.kept, ["data/binanceus/quotes/2026-11-12/s1.ndjson.gz"]);
    assert.ok(existsSync(join(root, "data/binanceus/quotes/2026-11-12/s1.ndjson.gz")));
    assert.match(readFileSync(join(root, "data/coinbase/bars1s/2026-11-11/BTC.ndjson"), "utf8"), /\[3,4\]/);
    assert.equal(existsSync(join(root, "manifests")), false);
    rmSync(root, { recursive: true, force: true });
  }
});

test("materialize refuses a bundle that does not match its manifest, and changes nothing", () => {
  const root = repo();
  put(root, "data/binanceus/quotes/2026-11-12/s1.ndjson.gz", "q1");
  put(root, "data/binanceus/quotes/2026-11-12/s2.ndjson.gz", "q2");
  const client = store();
  offload(root, { kind: "quotes", run: "9", client, log: quiet });
  const [name] = client.S.get("tape-2026-11-12").keys();
  client.S.get("tape-2026-11-12").set(name, Buffer.from("tampered"));
  assert.throws(() => materialize(root, { client, log: quiet }), /does not match/);
  assert.equal(existsSync(join(root, "data/binanceus/quotes/2026-11-12/s1.ndjson.gz")), false);
  assert.equal(existsSync(join(root, "data/binanceus/quotes/2026-11-12/s2.ndjson.gz")), false);
  assert.equal(existsSync(join(root, ".materialized.json")), false);
  rmSync(root, { recursive: true, force: true });
});

test("materialize refuses to overwrite a file entry that exists with other content", () => {
  const root = repo();
  put(root, "data/binanceus/quotes/2026-11-12/s1.ndjson.gz", "q1");
  const client = store();
  offload(root, { kind: "quotes", run: "3", client, log: quiet });
  put(root, "data/binanceus/quotes/2026-11-12/s1.ndjson.gz", "other");
  assert.throws(() => materialize(root, { client, log: quiet }), /already exists/);
  rmSync(root, { recursive: true, force: true });
});

test("check lists assets no manifest names and lines whose asset is gone", () => {
  const root = repo();
  put(root, "data/binanceus/quotes/2026-11-12/s1.ndjson.gz", "q1");
  const client = store();
  offload(root, { kind: "quotes", run: "4", client, log: quiet });
  client.S.get("tape-2026-11-12").set("stray.tar.gz", Buffer.from("s"));
  let r = check(root, { client, log: quiet });
  assert.deepEqual(r.unlisted, ["tape-2026-11-12/stray.tar.gz"]);
  assert.deepEqual(r.missing, []);
  client.S.get("tape-2026-11-12").delete("quotes-2026-11-12-4.tar.gz");
  r = check(root, { client, log: quiet });
  assert.deepEqual(r.missing, ["tape-2026-11-12/quotes-2026-11-12-4.tar.gz"]);
  rmSync(root, { recursive: true, force: true });
});
