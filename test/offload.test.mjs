// Offload and materialize on a temporary git repository with a fake release store. No network.
import { test } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { appendFileSync, copyFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { offload, pending, dayOfPath } from "../recorder/offload.mjs";
import { adopt, check, materialize, manifestLines } from "../recorder/materialize.mjs";
import { retryFiles } from "../recorder/manifest.mjs";

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
    remove(tag, name) {
      S.get(tag)?.delete(name);
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
  assert.deepEqual(retryFiles(root), []);
  rmSync(root, { recursive: true, force: true });
});

test("a failed upload or an asset missing at its size keeps quotes for git, parks bar chunks, and writes no manifest", () => {
  for (const opt of [{ failUpload: true }, { wrongSize: true }]) {
    const root = repo();
    put(root, "data/binanceus/quotes/2026-11-12/s1.ndjson.gz", "q");
    appendFileSync(join(root, "data/coinbase/bars1s/2026-11-11/BTC.ndjson"), "[3,4]\n");
    const client = store(opt);
    const q = offload(root, { kind: "quotes", run: "5", client, log: quiet });
    const b = offload(root, { kind: "bars", run: "5", client, log: quiet });
    assert.equal(q.bundles.length + b.bundles.length, 0);
    assert.deepEqual(q.kept, ["data/binanceus/quotes/2026-11-12/s1.ndjson.gz"]);
    assert.deepEqual(b.parked, ["data/coinbase/bars1s/2026-11-11/BTC.ndjson"]);
    assert.ok(existsSync(join(root, "data/binanceus/quotes/2026-11-12/s1.ndjson.gz")));
    assert.equal(readFileSync(join(root, "data/coinbase/bars1s/2026-11-11/BTC.ndjson"), "utf8"), "[1,2]\n[2,3]\n", "the day file is back at HEAD");
    assert.equal(readFileSync(join(root, "state/offload-retry/data/coinbase/bars1s/2026-11-11/BTC.ndjson"), "utf8"), "[3,4]\n");
    assert.equal(existsSync(join(root, "manifests")), false);
    assert.equal(client.S.get("tape-2026-11-12")?.size ?? 0, 0, "an uploaded asset that failed its check is removed");
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

test("bridge quotes get their own manifest files, and materialize reads both", () => {
  const root = repo();
  const client = store();
  put(root, "data/binanceus/quotes/2026-11-12/a.ndjson.gz", "main");
  put(root, "data/binanceus/quotes-bridge/2026-11-12/b.ndjson.gz", "bridge");
  offload(root, { kind: "quotes", run: "1", client, log: quiet });
  offload(root, { kind: "bridge", run: "2", client, log: quiet });
  assert.ok(existsSync(join(root, "manifests/2026-11-12.ndjson")));
  assert.ok(existsSync(join(root, "manifests/bridge/2026-11-12.ndjson")));
  assert.deepEqual(manifestLines(root).map((m) => m.kind), ["quotes", "bridge"]);
  materialize(root, { client, log: quiet });
  assert.equal(readFileSync(join(root, "data/binanceus/quotes-bridge/2026-11-12/b.ndjson.gz"), "utf8"), "bridge");
  assert.equal(readFileSync(join(root, "data/binanceus/quotes/2026-11-12/a.ndjson.gz"), "utf8"), "main");
  rmSync(root, { recursive: true, force: true });
});

const day12b = "data/coinbase/bars1s/2026-11-12/BTC.ndjson";

test("bars: a chunk parked by a failed run goes out first with the next run, and the day rebuilds in order", () => {
  const root = repo();
  const client = store();
  const upload = client.upload;
  put(root, day12b, "[1,A]\n");
  offload(root, { kind: "bars", run: "A", client, log: quiet });
  commitAll(root, "A");
  client.upload = () => {
    throw new Error("down");
  };
  put(root, day12b, "[2,B]\n");
  const b = offload(root, { kind: "bars", run: "B", client, log: quiet });
  assert.deepEqual(b.parked, [day12b]);
  assert.equal(existsSync(join(root, day12b)), false, "git never gets B's rows at the day file's path");
  commitAll(root, "B");
  client.upload = upload;
  put(root, day12b, "[3,C]\n");
  const c = offload(root, { kind: "bars", run: "C", client, log: quiet });
  assert.equal(c.bundles.length, 1);
  assert.deepEqual(c.bundles[0].files.map((f) => [f.offset, f.bytes]), [[6, 12]]);
  assert.equal(existsSync(join(root, "state/offload-retry", day12b)), false);
  commitAll(root, "C");
  materialize(root, { client, log: quiet });
  assert.equal(readFileSync(join(root, day12b), "utf8"), "[1,A]\n[2,B]\n[3,C]\n");
  rmSync(root, { recursive: true, force: true });
});

test("materialize applies each chunk once, even with its state file lost, and refuses a chunk out of place", () => {
  const root = repo();
  const client = store();
  put(root, day12b, "[1,a]\n");
  offload(root, { kind: "bars", run: "1", client, log: quiet });
  put(root, day12b, "[2,b]\n");
  offload(root, { kind: "bars", run: "2", client, log: quiet });
  commitAll(root, "two runs");
  materialize(root, { client, log: quiet });
  rmSync(join(root, ".materialized.json"));
  const again = materialize(root, { client, log: quiet });
  assert.equal(again.files, 0);
  assert.equal(again.skipped, 2);
  assert.equal(readFileSync(join(root, day12b), "utf8"), "[1,a]\n[2,b]\n");
  writeFileSync(join(root, day12b), "[9,z]\n");
  rmSync(join(root, ".materialized.json"));
  assert.throws(() => materialize(root, { client, log: quiet }), /does not hold the chunk before|ends at/);
  rmSync(root, { recursive: true, force: true });
});

test("manifest lines written before offsets existed get the implied offsets", () => {
  const root = repo();
  const client = store();
  const day11 = "data/coinbase/bars1s/2026-11-11/BTC.ndjson";
  appendFileSync(join(root, day11), "[3,4]\n");
  offload(root, { kind: "bars", run: "1", client, log: quiet });
  appendFileSync(join(root, day11), "[4,5]\n");
  offload(root, { kind: "bars", run: "2", client, log: quiet });
  const mf = join(root, "manifests/2026-11-11.ndjson");
  const lines = readFileSync(mf, "utf8").trim().split("\n").map((l) => JSON.parse(l));
  assert.deepEqual(lines.map((l) => l.files[0].offset), [12, 18]);
  for (const l of lines) delete l.files[0].offset; // as the first offload version wrote them
  writeFileSync(mf, lines.map((l) => JSON.stringify(l)).join("\n") + "\n");
  commitAll(root, "legacy");
  materialize(root, { client, log: quiet });
  assert.equal(readFileSync(join(root, day11), "utf8"), "[1,2]\n[2,3]\n[3,4]\n[4,5]\n");
  rmSync(root, { recursive: true, force: true });
});

test("an asset whose manifest line never reached git is listed by check and restored by adopt", () => {
  const root = repo();
  const client = store();
  put(root, "data/binanceus/quotes/2026-11-12/s1.ndjson.gz", "q1");
  put(root, day12b, "[1,a]\n");
  offload(root, { kind: "quotes", run: "7", client, log: quiet });
  offload(root, { kind: "bars", run: "7", client, log: quiet });
  // as if the run was killed after removing its files and before writing the lines
  rmSync(join(root, "manifests"), { recursive: true, force: true });
  assert.equal(check(root, { client, log: quiet }).unlisted.length, 2);
  assert.equal(adopt(root, { client, log: quiet }).adopted.length, 2);
  assert.deepEqual(check(root, { client, log: quiet }).unlisted, []);
  materialize(root, { client, log: quiet });
  assert.equal(readFileSync(join(root, day12b), "utf8"), "[1,a]\n");
  assert.equal(readFileSync(join(root, "data/binanceus/quotes/2026-11-12/s1.ndjson.gz"), "utf8"), "q1");
  assert.equal(adopt(root, { client, log: quiet }).adopted.length, 0, "a second adopt finds nothing");
  rmSync(root, { recursive: true, force: true });
});

test("adopt refuses a quotes asset whose files git already has", () => {
  const root = repo();
  const client = store();
  put(root, "data/binanceus/quotes/2026-11-12/s1.ndjson.gz", "q1");
  offload(root, { kind: "quotes", run: "8", client, log: quiet });
  rmSync(join(root, "manifests"), { recursive: true, force: true });
  put(root, "data/binanceus/quotes/2026-11-12/s1.ndjson.gz", "q1"); // git kept the file too
  const r = adopt(root, { client, log: quiet });
  assert.deepEqual(r.adopted, []);
  assert.deepEqual(r.refused, ["tape-2026-11-12/quotes-2026-11-12-8.tar.gz"]);
  rmSync(root, { recursive: true, force: true });
});

test("bars: a run whose manifest line was lost is adopted by the next run before it takes an offset", () => {
  const root = repo();
  const client = store();
  put(root, day12b, "[1,A]\n");
  offload(root, { kind: "bars", run: "1", client, log: quiet });
  const kept = readFileSync(join(root, "manifests/2026-11-12.ndjson"), "utf8");
  commitAll(root, "1");
  put(root, day12b, "[2,B]\n");
  offload(root, { kind: "bars", run: "2", client, log: quiet });
  writeFileSync(join(root, "manifests/2026-11-12.ndjson"), kept); // run 2's commit lost: the checkout is as after run 1
  put(root, day12b, "[3,C]\n");
  const logs = [];
  const c = offload(root, { kind: "bars", run: "3", client, log: (l) => logs.push(l) });
  assert.ok(logs.some((l) => /adopted tape-2026-11-12\/bars-2026-11-12-2/.test(l)));
  assert.deepEqual(c.bundles[0].files.map((f) => f.offset), [12]);
  commitAll(root, "3");
  materialize(root, { client, log: quiet });
  assert.equal(readFileSync(join(root, day12b), "utf8"), "[1,A]\n[2,B]\n[3,C]\n");
  rmSync(root, { recursive: true, force: true });
});

test("bars: an asset whose rows are still parked is a duplicate and is removed", () => {
  const root = repo();
  const client = store();
  put(root, day12b, "[1,A]\n");
  offload(root, { kind: "bars", run: "1", client, log: quiet });
  commitAll(root, "1");
  // run 2's upload lands, then its check and its clean-up fail, so its rows stay parked and its asset stays
  const { assets, remove } = client;
  client.assets = () => {
    throw new Error("cut off");
  };
  client.remove = () => {};
  put(root, day12b, "[2,B]\n");
  assert.deepEqual(offload(root, { kind: "bars", run: "2", client, log: quiet }).parked, [day12b]);
  Object.assign(client, { assets, remove });
  assert.ok(client.S.get("tape-2026-11-12").has("bars-2026-11-12-2.tar.gz"));
  commitAll(root, "2 cut off");
  put(root, day12b, "[3,C]\n");
  offload(root, { kind: "bars", run: "3", client, log: quiet });
  assert.equal(client.S.get("tape-2026-11-12").has("bars-2026-11-12-2.tar.gz"), false);
  commitAll(root, "3");
  materialize(root, { client, log: quiet });
  assert.equal(readFileSync(join(root, day12b), "utf8"), "[1,A]\n[2,B]\n[3,C]\n");
  rmSync(root, { recursive: true, force: true });
});

test("bars: every day is parked before the first upload, so a hang on one day leaves no rows at another's path", () => {
  const root = repo();
  const client = store();
  const day13 = "data/coinbase/bars1s/2026-11-13/BTC.ndjson";
  put(root, day12b, "[1,a]\n");
  put(root, day13, "[1,b]\n");
  let seen = null;
  client.ensureRelease = () => {
    // what git would commit if the step were killed here
    seen ??= git(root, "status", "--porcelain", "-uall", "--", "data", "state").trim().split("\n").sort();
    throw new Error("hang");
  };
  offload(root, { kind: "bars", run: "1", client, log: quiet });
  assert.deepEqual(seen, ["?? state/offload-retry/" + day12b, "?? state/offload-retry/" + day13].sort());
  rmSync(root, { recursive: true, force: true });
});

test("materialize finishes an append that was cut off part way, and ignores a half-written park", () => {
  const root = repo();
  const client = store();
  put(root, day12b, "[1,a]\n[2,b]\n");
  offload(root, { kind: "bars", run: "1", client, log: quiet });
  commitAll(root, "1");
  put(root, day12b, "[1,a]\n[2"); // as if the append stopped part way
  writeFileSync(join(root, ".materialized.json"), JSON.stringify({ applied: [] }));
  materialize(root, { client, log: quiet });
  assert.equal(readFileSync(join(root, day12b), "utf8"), "[1,a]\n[2,b]\n");
  put(root, "state/offload-retry/" + day12b + ".tmp", "[9,z]\n");
  assert.deepEqual(retryFiles(root), []);
  rmSync(root, { recursive: true, force: true });
});

test("materialize refuses a manifest line with an unexpected release or asset name", () => {
  const root = repo();
  put(root, "manifests/2026-11-12.ndjson", JSON.stringify({ asset: "../x.tar.gz", release: "tape-2026-11-12", kind: "quotes", size: 1, sha256: "0", files: [] }) + "\n");
  assert.throws(() => materialize(root, { client: store(), log: quiet }), /unexpected release or asset name/);
  rmSync(root, { recursive: true, force: true });
});
