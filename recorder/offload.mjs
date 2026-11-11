// Offload: move a run's quote session files and one-second bar chunks out of git into release assets, so the
// repository stops growing by about 30 MB a day (CHANGES 2026-11-12). Only a small manifest line per bundle is
// committed: manifests/<day>.ndjson, one JSON object per line, in run order.
//
//   node recorder/offload.mjs <root> --kind quotes|bars --run <run id>
//
// For each UTC day touched by this run, the new data under the kind's directories becomes one tar.gz bundle,
// uploaded to the release tagged tape-<day> (created if missing). An untracked file is bundled whole. A tracked
// file that this run appended to (a one-second bar day file that was already in git before the switch) is
// bundled as its appended tail only. Files leave the working tree only after the upload is checked against the
// release's asset list; anything that fails stays where it is and is committed to git as before, so nothing is
// lost. recorder/materialize.mjs rebuilds the old layout byte for byte from the manifests.
import { execFileSync } from "node:child_process";
import { appendFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, unlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { sha256 } from "./lib.mjs";

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

/** What this run added under the kind's directories: [{path, mode: "file"|"append", bytes(Buffer)}]. */
export function pending(root, kind) {
  const dirs = KINDS[kind].filter((d) => existsSync(join(root, d)));
  if (!dirs.length) return [];
  const out = [];
  const untracked = git(root, ["ls-files", "--others", "--exclude-standard", "-z", "--", ...dirs]).split("\0").filter(Boolean);
  for (const p of untracked) {
    if (!dayOfPath(p)) continue;
    // a one-second bar file is a day's rows appended run by run; materialize appends its chunks in order
    out.push({ path: p, mode: kind === "bars" ? "append" : "file", bytes: readFileSync(join(root, p)) });
  }
  const modified = git(root, ["diff", "--name-only", "-z", "--", ...dirs]).split("\0").filter(Boolean);
  for (const p of modified) {
    if (kind !== "bars" || !dayOfPath(p)) continue; // a changed quote file is not expected: leave it to git
    const head = git(root, ["show", `HEAD:${p}`], { buffer: true });
    const cur = readFileSync(join(root, p));
    if (cur.length <= head.length || !cur.subarray(0, head.length).equals(head)) continue; // not a pure append
    out.push({ path: p, mode: "append", bytes: cur.subarray(head.length), tracked: true });
  }
  return out;
}

/** The default release client: the gh CLI with the job's token. */
export function ghClient(repo) {
  const gh = (args) => execFileSync("gh", args, { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], env: { ...process.env, GH_REPO: repo } });
  return {
    ensureRelease(tag, day) {
      try {
        gh(["release", "view", tag, "--json", "tagName"]);
        return;
      } catch {}
      try {
        gh(["release", "create", tag, "--title", `tape ${day}`, "--notes", `Recorded data for ${day} that is kept out of git; the bundles are listed in manifests/${day}.ndjson. Rebuild the files with: node recorder/materialize.mjs .`]);
      } catch (e) {
        gh(["release", "view", tag, "--json", "tagName"]); // created meanwhile by another job, or a real failure
      }
    },
    upload(tag, file) {
      gh(["release", "upload", tag, file]);
    },
    assets(tag) {
      return JSON.parse(gh(["release", "view", tag, "--json", "assets"])).assets.map((a) => ({ name: a.name, size: a.size }));
    },
  };
}

/**
 * Offload one kind. Returns {bundles: [...manifest lines], kept: [paths left for git]}. `client` is injectable for
 * tests; `tar` makes the bundle from a staging directory.
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
  for (const [day, list] of [...byDay].sort()) {
    const tag = tagOf(day);
    const name = `${kind}-${day}-${run}.tar.gz`;
    const stage = mkdtempSync(join(tmpdir(), "offload-"));
    try {
      for (const it of list) {
        const p = join(stage, "files", it.path);
        mkdirSync(dirname(p), { recursive: true });
        writeFileSync(p, it.bytes);
      }
      const file = join(stage, name);
      execFileSync("tar", ["-czf", file, "-C", join(stage, "files"), "."], { stdio: "ignore" });
      const buf = readFileSync(file);
      client.ensureRelease(tag, day);
      client.upload(tag, file);
      const seen = client.assets(tag).find((a) => a.name === name);
      if (!seen || seen.size !== buf.length) throw new Error(`asset ${name} not found at its size after upload`);
      const line = {
        asset: name,
        release: tag,
        kind,
        run: String(run),
        at: new Date(now()).toISOString(),
        size: buf.length,
        sha256: sha256(buf),
        files: list.map((it) => ({ path: it.path, mode: it.mode, bytes: it.bytes.length, sha256: sha256(it.bytes) })),
      };
      // the manifest line first: if removing a file failed, git would commit it and the line, never neither
      const mf = join(root, MANIFESTS[kind], `${day}.ndjson`);
      mkdirSync(dirname(mf), { recursive: true });
      appendFileSync(mf, JSON.stringify(line) + "\n");
      for (const it of list) {
        if (it.tracked) git(root, ["checkout", "HEAD", "--", it.path]);
        else unlinkSync(join(root, it.path));
      }
      bundles.push(line);
      log(`offloaded ${list.length} files of ${day} as ${tag}/${name} (${buf.length} bytes)`);
    } catch (e) {
      kept.push(...list.map((it) => it.path));
      log(`offload of ${day} failed, its ${list.length} files stay for git: ${e.message}`);
    } finally {
      rmSync(stage, { recursive: true, force: true });
    }
  }
  return { bundles, kept };
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
  console.log(`offload ${arg("--kind")}: ${r.bundles.length} bundles, ${r.kept.length} files left for git`);
}
