// Offload: move a run's quote session files and one-second bar chunks out of git into release assets, so the
// repository stops growing by about 30 MB a day (CHANGES 2026-11-12, 2026-11-12c). Only a small manifest line per
// bundle is committed: manifests/<day>.ndjson, one JSON object per line, in run order.
//
//   node recorder/offload.mjs <root> --kind quotes|bars|bridge --run <run id>
//
// For each UTC day touched by this run, the new data under the kind's directories becomes one tar.gz bundle,
// uploaded to the release tagged tape-<day> (created if missing). The bundle also carries its own manifest line as
// manifest.json, so an asset whose line never reached git can be adopted later (materialize.mjs --adopt).
//
// Quotes: an untracked session file is bundled whole. If anything fails it stays where it is and git commits it as
// before.
//
// Bars: a day file grows run by run, so each chunk is bundled with its offset in the rebuilt file (see
// manifest.mjs). Before the upload, the chunk moves to state/offload-retry/<path> and the file goes back to its
// HEAD state (or away, if untracked), so git never holds a chunk out of place: a failed or killed upload leaves the
// chunk there for git, and the next run sends it first, joined to its own rows.
//
// After a checked upload the local copies go first and the manifest line last. A run killed between the two leaves
// an asset no line names, which --check lists and --adopt restores; the other order would let git commit the line
// and the data both.
import { execFileSync } from "node:child_process";
import { appendFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, renameSync, rmSync, unlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { sha256 } from "./lib.mjs";
import { ASSET_RE, RETRY, dayEnds, manifestLines, retryFiles } from "./manifest.mjs";

export const KINDS = {
  quotes: ["data/binanceus/quotes", "data/coinbase/quotes", "data/kraken/quotes"],
  bars: ["data/coinbase/bars1s", "data/kraken/bars1s"],
  bridge: ["data/binanceus/quotes-bridge", "data/coinbase/quotes-bridge", "data/kraken/quotes-bridge"],
};
// The bridge workflow commits at the same time as the record chain, so its manifest lines go to their own files
// and the two never edit the same file (a rebase would otherwise conflict).
export const MANIFESTS = { quotes: "manifests", bars: "manifests", bridge: "manifests/bridge" };
export const tagOf = (day) => `tape-${day}`;
// data/<venue>/<stream>/<YYYY-MM-DD>/<file>: the day is the fourth segment
export const dayOfPath = (p) => {
  const m = /^data\/[^/]+\/[^/]+\/(\d{4}-\d{2}-\d{2})\//.exec(p);
  return m ? m[1] : null;
};

const git = (root, args, opts = {}) => execFileSync("git", ["-C", root, ...args], { encoding: opts.buffer ? null : "utf8", maxBuffer: 1 << 30, stdio: ["ignore", "pipe", "pipe"] });

/**
 * What this run has to send for one kind: [{path, mode: "file"|"append", bytes(Buffer), offset?, tracked?, retry?,
 * fresh?}]. For bars, a parked chunk from an earlier run comes first, joined to this run's rows for the same file.
 */
export function pending(root, kind) {
  const dirs = KINDS[kind].filter((d) => existsSync(join(root, d)));
  const fresh = new Map();
  if (dirs.length) {
    const untracked = git(root, ["ls-files", "--others", "--exclude-standard", "-z", "--", ...dirs]).split("\0").filter(Boolean);
    for (const p of untracked) if (dayOfPath(p)) fresh.set(p, { path: p, bytes: readFileSync(join(root, p)) });
    if (kind === "bars") {
      const modified = git(root, ["diff", "--name-only", "-z", "--", ...dirs]).split("\0").filter(Boolean);
      for (const p of modified) {
        if (!dayOfPath(p)) continue;
        const head = git(root, ["show", `HEAD:${p}`], { buffer: true });
        const cur = readFileSync(join(root, p));
        if (cur.length <= head.length || !cur.subarray(0, head.length).equals(head)) continue; // not a pure append: leave it to git
        fresh.set(p, { path: p, bytes: cur.subarray(head.length), tracked: true });
      }
    }
  }
  if (kind !== "bars") return [...fresh.values()].map((it) => ({ ...it, mode: "file", fresh: true }));
  const out = new Map();
  for (const r of retryFiles(root)) {
    if (!KINDS.bars.some((d) => r.path.startsWith(d + "/")) || !dayOfPath(r.path)) continue;
    out.set(r.path, { path: r.path, mode: "append", bytes: readFileSync(join(root, r.file)), retry: r.file });
  }
  for (const [p, it] of fresh) {
    const prev = out.get(p);
    out.set(p, { path: p, mode: "append", bytes: prev ? Buffer.concat([prev.bytes, it.bytes]) : it.bytes, tracked: it.tracked, retry: prev?.retry, fresh: true });
  }
  return [...out.values()].sort((a, b) => (a.path < b.path ? -1 : 1));
}

/** The default release client: the gh CLI with the job's token, REST only. */
export function ghClient(repo) {
  const gh = (args) => execFileSync("gh", args, { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], env: { ...process.env, GH_REPO: repo } });
  const release = (tag) => JSON.parse(gh(["api", `repos/${repo}/releases/tags/${tag}`]));
  // the release's own asset list may be cut short on a busy day (about 150 assets), so page the assets endpoint
  const list = (tag) =>
    gh(["api", "--paginate", `repos/${repo}/releases/${release(tag).id}/assets?per_page=100`, "--jq", ".[] | {id, name, size, digest}"])
      .split("\n")
      .filter(Boolean)
      .map((l) => JSON.parse(l));
  return {
    ensureRelease(tag, day) {
      try {
        release(tag);
        return;
      } catch {}
      try {
        gh(["release", "create", tag, "--title", `tape ${day}`, "--notes", `Recorded data for ${day} that is kept out of git; the bundles are listed in manifests/${day}.ndjson. Rebuild the files with: node recorder/materialize.mjs .`]);
      } catch (e) {
        release(tag); // created meanwhile by another job, or a real failure
      }
    },
    upload(tag, file) {
      gh(["release", "upload", tag, file]);
    },
    assets(tag) {
      return list(tag).map((a) => ({ name: a.name, size: a.size, digest: a.digest ?? null }));
    },
    remove(tag, name) {
      const a = list(tag).find((x) => x.name === name);
      if (a) gh(["api", "-X", "DELETE", `repos/${repo}/releases/assets/${a.id}`]);
    },
    download(tag, name, dir) {
      const a = list(tag).find((x) => x.name === name);
      if (!a) throw new Error(`${tag}/${name}: no such asset`);
      const p = join(dir, name);
      writeFileSync(p, execFileSync("gh", ["api", "-H", "Accept: application/octet-stream", `repos/${repo}/releases/assets/${a.id}`], { maxBuffer: 1 << 30, stdio: ["ignore", "pipe", "pipe"], env: { ...process.env, GH_REPO: repo } }));
      return p;
    },
  };
}

// write then rename, so a parked chunk is never half written
const writeAtomic = (p, bytes) => {
  mkdirSync(dirname(p), { recursive: true });
  writeFileSync(p + ".tmp", bytes);
  renameSync(p + ".tmp", p);
};

/**
 * Before a bars day gets offsets: deal with bar assets in its release that no manifest line names (a run whose
 * commit was lost, or one killed between removing its chunks and writing its line). Their chunk would otherwise be
 * overtaken by this run's chunk at the same offset and drop out of the rebuilt file. An asset whose chunks are held,
 * byte for byte, at the front of this run's parked chunks is a duplicate (the run that sent it was killed before it
 * removed its park) and is deleted. An asset whose chunks all start where their files end and are not held gets its
 * line written now (adopted), and this run's chunks go after it. Anything
 * else holds the day: its chunks stay parked (in git) until someone runs materialize.mjs --check and --adopt.
 * Returns {adopted, removed, blocked}.
 */
export function reconcile(root, { day, tag, list, client, log = () => {} }) {
  let assets;
  try {
    assets = client.assets(tag);
  } catch {
    return { adopted: [], removed: [], blocked: null }; // no release yet, so no orphans
  }
  const named = new Set(manifestLines(root, { from: day, to: day }).map((m) => m.asset));
  const orphans = assets.filter((a) => a.name.startsWith(`bars-${day}-`) && !named.has(a.name)).map((a) => a.name).sort();
  const adopted = [];
  const removed = [];
  if (!orphans.length) return { adopted, removed, blocked: null };
  const parkedBytes = new Map(list.map((it) => [it.path, it.bytes]));
  const heads = [];
  for (const name of orphans) {
    if (!ASSET_RE.test(name)) return { adopted, removed, blocked: `unexpected asset name ${name}` };
    const work = mkdtempSync(join(tmpdir(), "reconcile-"));
    try {
      const file = client.download(tag, name, work);
      const buf = readFileSync(file);
      const dir = join(work, "x");
      mkdirSync(dir);
      execFileSync("tar", ["-xzf", file, "-C", dir, "--no-same-owner", "--no-same-permissions"], { stdio: "ignore" });
      if (!existsSync(join(dir, "manifest.json"))) return { adopted, removed, blocked: `${name} carries no manifest line` };
      const head = JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8"));
      if (head.asset !== name || head.release !== tag || head.kind !== "bars") return { adopted, removed, blocked: `${name}: its manifest line names another asset` };
      const chunks = head.files.map((f) => {
        const bytes = readFileSync(join(dir, f.path));
        if (f.mode !== "append" || bytes.length !== f.bytes || sha256(bytes) !== f.sha256) throw new Error(`${name}: ${f.path} does not match its entry`);
        return { ...f, data: bytes };
      });
      heads.push({ name, head, chunks, size: buf.length, sha256: sha256(buf) });
    } finally {
      rmSync(work, { recursive: true, force: true });
    }
  }
  // oldest chunk first, so two orphans of one file are taken in order
  const first = (h) => Math.min(...h.chunks.map((c) => c.offset));
  heads.sort((a, b) => first(a) - first(b));
  for (const h of heads) {
    const end = dayEnds(root, day, KINDS.bars);
    // a chunk this run still holds at the front of its parked rows is a duplicate; any other chunk that starts
    // where its file ends came before this run's rows
    const held = (c) => parkedBytes.get(c.path)?.subarray(0, c.bytes).equals(c.data) ?? false;
    const fits = h.chunks.every((c) => c.offset === end(c.path) && !held(c));
    const dup = h.chunks.every((c) => c.offset === end(c.path) && held(c));
    if (fits) {
      const mf = join(root, MANIFESTS.bars, `${day}.ndjson`);
      mkdirSync(dirname(mf), { recursive: true });
      appendFileSync(mf, JSON.stringify({ ...h.head, size: h.size, sha256: h.sha256 }) + "\n");
      adopted.push(h.name);
      log(`adopted ${tag}/${h.name}, whose line was missing`);
    } else if (dup) {
      client.remove(tag, h.name);
      removed.push(h.name);
      log(`removed ${tag}/${h.name}: its rows are still parked and go out with this run`);
    } else return { adopted, removed, blocked: `${h.name} has no manifest line and does not fit where its files end` };
  }
  return { adopted, removed, blocked: null };
}

/**
 * Offload one kind. Returns {bundles: [...manifest lines], kept: [paths left for git], parked: [bar paths whose
 * chunk waits in state/offload-retry]}. `client` is injectable for tests.
 */
export function offload(root, { kind, run, client, now = Date.now, log = console.log }) {
  if (!KINDS[kind]) throw new Error(`unknown kind ${kind}`);
  if (!run) throw new Error("--run is required");
  const items = pending(root, kind);
  const byDay = new Map();
  for (const it of items) {
    const d = dayOfPath(it.path);
    if (!byDay.has(d)) byDay.set(d, []);
    byDay.get(d).push(it);
  }
  const bundles = [];
  const kept = [];
  const parked = [];
  // bars: park every day's chunks before any network call, so a hang or kill on one day never leaves another
  // day's rows at their path for git
  if (kind === "bars")
    for (const it of items) {
      writeAtomic(join(root, RETRY, it.path), it.bytes);
      it.retry = `${RETRY}/${it.path}`;
      if (it.fresh) {
        if (it.tracked) git(root, ["checkout", "HEAD", "--", it.path]);
        else unlinkSync(join(root, it.path));
      }
    }
  for (const [day, list] of [...byDay].sort()) {
    const tag = tagOf(day);
    const name = `${kind}-${day}-${run}.tar.gz`;
    if (kind === "bars") {
      try {
        const r = reconcile(root, { day, tag, list, client, log });
        if (r.blocked) throw new Error(r.blocked);
        const end = dayEnds(root, day, KINDS.bars);
        for (const it of list) it.offset = end(it.path);
      } catch (e) {
        parked.push(...list.map((it) => it.path));
        log(`offload of ${day} held, its ${list.length} bar chunks wait in ${RETRY}: ${e.message}`);
        continue;
      }
    }
    const stage = mkdtempSync(join(tmpdir(), "offload-"));
    let uploaded = false;
    try {
      for (const it of list) {
        const p = join(stage, "files", it.path);
        mkdirSync(dirname(p), { recursive: true });
        writeFileSync(p, it.bytes);
      }
      const files = list.map((it) => ({ path: it.path, mode: it.mode, ...(it.mode === "append" ? { offset: it.offset } : {}), bytes: it.bytes.length, sha256: sha256(it.bytes) }));
      const head = { asset: name, release: tag, kind, run: String(run), at: new Date(now()).toISOString(), files };
      writeFileSync(join(stage, "files", "manifest.json"), JSON.stringify(head) + "\n");
      const file = join(stage, name);
      execFileSync("tar", ["-czf", file, "-C", join(stage, "files"), "."], { stdio: "ignore" });
      const buf = readFileSync(file);
      const digest = sha256(buf);
      client.ensureRelease(tag, day);
      uploaded = true;
      client.upload(tag, file);
      const seen = client.assets(tag).find((a) => a.name === name);
      if (!seen || seen.size !== buf.length) throw new Error(`asset ${name} not found at its size after upload`);
      if (seen.digest && seen.digest !== `sha256:${digest}`) throw new Error(`asset ${name} has digest ${seen.digest}, not sha256:${digest}`);
      const line = { asset: name, release: tag, kind, run: String(run), at: head.at, size: buf.length, sha256: digest, files };
      // local copies first, the line last (see the header)
      for (const it of list) {
        if (it.retry) rmSync(join(root, it.retry), { force: true });
        else unlinkSync(join(root, it.path));
      }
      const mf = join(root, MANIFESTS[kind], `${day}.ndjson`);
      mkdirSync(dirname(mf), { recursive: true });
      appendFileSync(mf, JSON.stringify(line) + "\n");
      bundles.push(line);
      log(`offloaded ${list.length} files of ${day} as ${tag}/${name} (${buf.length} bytes)`);
    } catch (e) {
      if (uploaded)
        try {
          client.remove(tag, name);
        } catch {}
      if (kind === "bars") {
        parked.push(...list.map((it) => it.path));
        log(`offload of ${day} failed, its ${list.length} bar chunks wait in ${RETRY} for the next run: ${e.message}`);
      } else {
        kept.push(...list.map((it) => it.path));
        log(`offload of ${day} failed, its ${list.length} files stay for git: ${e.message}`);
      }
    } finally {
      rmSync(stage, { recursive: true, force: true });
    }
  }
  return { bundles, kept, parked };
}

const isMain = process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1]);
if (isMain) {
  const root = resolve(process.argv[2] ?? ".");
  const arg = (k) => {
    const i = process.argv.indexOf(k);
    return i > 0 ? process.argv[i + 1] : undefined;
  };
  const repo = process.env.GITHUB_REPOSITORY ?? "mrastro18/faith-tape";
  const r = offload(root, { kind: arg("--kind"), run: arg("--run") ?? process.env.GITHUB_RUN_ID, client: ghClient(repo) });
  console.log(`offload ${arg("--kind")}: ${r.bundles.length} bundles, ${r.kept.length} files left for git, ${r.parked.length} bar chunks parked`);
}
