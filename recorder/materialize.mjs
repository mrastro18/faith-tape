// Materialize: rebuild the files that recorder/offload.mjs moved into release assets, so a checkout reads exactly
// as it would have if every run had committed its data to git (CHANGES 2026-11-12).
//
//   node recorder/materialize.mjs <root> [--from YYYY-MM-DD] [--to YYYY-MM-DD] [--check]
//
// Reads manifests/<day>.ndjson in day order and, within a day, line order (the order the runs wrote them). For
// each bundle not yet applied it downloads the asset, checks its size and sha256, unpacks it, checks each file's
// sha256, then applies the entries: a "file" entry is written and must be absent or byte-identical; an "append"
// entry is added to the end of its file, which rebuilds a one-second bar day file chunk by chunk. Applied bundles
// are listed in .materialized.json (not tracked), so a second run only applies what is new. --from/--to limit
// the days. --check applies nothing and lists release assets that no manifest names and manifest lines whose
// asset is missing.
//
// Run it on a checkout you do not commit from: the rebuilt files are untracked or modified, by design.
import { execFileSync } from "node:child_process";
import { appendFileSync, existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { sha256 } from "./lib.mjs";

const STATE = ".materialized.json";

export function manifestLines(root, { from, to } = {}) {
  const dir = join(root, "manifests");
  if (!existsSync(dir)) return [];
  const days = readdirSync(dir)
    .filter((f) => /^\d{4}-\d{2}-\d{2}\.ndjson$/.test(f))
    .map((f) => f.slice(0, 10))
    .filter((d) => (!from || d >= from) && (!to || d <= to))
    .sort();
  const out = [];
  for (const d of days)
    for (const l of readFileSync(join(dir, `${d}.ndjson`), "utf8").split("\n"))
      if (l.trim()) out.push({ day: d, ...JSON.parse(l) });
  return out;
}

export function ghReader(repo) {
  const gh = (args) => execFileSync("gh", args, { encoding: "utf8", maxBuffer: 1 << 28, stdio: ["ignore", "pipe", "pipe"], env: { ...process.env, GH_REPO: repo } });
  return {
    download(tag, name, dir) {
      gh(["release", "download", tag, "--pattern", name, "--dir", dir, "--clobber"]);
      return join(dir, name);
    },
    releases() {
      return gh(["release", "list", "--limit", "2000", "--json", "tagName", "--jq", ".[].tagName"]).split("\n").filter((t) => t.startsWith("tape-"));
    },
    assets(tag) {
      return JSON.parse(gh(["release", "view", tag, "--json", "assets"])).assets.map((a) => ({ name: a.name, size: a.size }));
    },
  };
}

// Path entries come from a committed manifest, but are still checked: relative, under data/, no "..".
const safe = (p) => typeof p === "string" && p.startsWith("data/") && !p.split("/").includes("..") && !p.includes("\\");

export function materialize(root, { client, from, to, log = console.log } = {}) {
  const statePath = join(root, STATE);
  const state = existsSync(statePath) ? JSON.parse(readFileSync(statePath, "utf8")) : { applied: [] };
  const done = new Set(state.applied);
  let applied = 0;
  let files = 0;
  for (const m of manifestLines(root, { from, to })) {
    if (done.has(m.asset)) continue;
    const work = mkdtempSync(join(tmpdir(), "materialize-"));
    try {
      const file = client.download(m.release, m.asset, work);
      const buf = readFileSync(file);
      if (buf.length !== m.size || sha256(buf) !== m.sha256) throw new Error(`${m.asset}: size or sha256 does not match its manifest line`);
      const out = join(work, "x");
      mkdirSync(out);
      execFileSync("tar", ["-xzf", file, "-C", out, "--no-same-owner", "--no-same-permissions"], { stdio: "ignore" });
      // check every entry before touching the checkout, so a bad bundle changes nothing
      const parts = m.files.map((f) => {
        if (!safe(f.path)) throw new Error(`${m.asset}: unsafe path ${f.path}`);
        const bytes = readFileSync(join(out, f.path));
        if (bytes.length !== f.bytes || sha256(bytes) !== f.sha256) throw new Error(`${m.asset}: ${f.path} does not match its manifest entry`);
        const target = join(root, f.path);
        if (f.mode === "file" && existsSync(target) && !readFileSync(target).equals(bytes))
          throw new Error(`${m.asset}: ${f.path} already exists with other content`);
        if (f.mode !== "file" && f.mode !== "append") throw new Error(`${m.asset}: unknown mode ${f.mode}`);
        return { ...f, target, data: bytes };
      });
      for (const p of parts) {
        mkdirSync(dirname(p.target), { recursive: true });
        if (p.mode === "file") {
          if (!existsSync(p.target)) writeFileSync(p.target, p.data);
        } else appendFileSync(p.target, p.data);
        files++;
      }
      done.add(m.asset);
      state.applied = [...done];
      writeFileSync(statePath, JSON.stringify(state) + "\n"); // after each bundle, so an interrupted run resumes
      applied++;
    } finally {
      rmSync(work, { recursive: true, force: true });
    }
  }
  log(`materialized ${applied} bundles (${files} files); ${done.size} applied in all`);
  return { applied, files };
}

export function check(root, { client, log = console.log } = {}) {
  const lines = manifestLines(root);
  const named = new Set(lines.map((m) => `${m.release}/${m.asset}`));
  const present = new Map();
  for (const tag of client.releases()) for (const a of client.assets(tag)) present.set(`${tag}/${a.name}`, a.size);
  const unlisted = [...present.keys()].filter((k) => !named.has(k)).sort();
  const missing = lines.filter((m) => present.get(`${m.release}/${m.asset}`) !== m.size).map((m) => `${m.release}/${m.asset}`);
  log(`${lines.length} manifest lines, ${present.size} assets; ${unlisted.length} assets no manifest names, ${missing.length} lines with no asset at their size`);
  for (const k of unlisted) log(`unlisted: ${k}`);
  for (const k of missing) log(`missing: ${k}`);
  return { unlisted, missing };
}

const isMain = process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1]);
if (isMain) {
  const root = resolve(process.argv[2] ?? ".");
  const arg = (k) => {
    const i = process.argv.indexOf(k);
    return i > 0 ? process.argv[i + 1] : undefined;
  };
  const client = ghReader(process.env.GITHUB_REPOSITORY ?? "mrastro18/faith-tape");
  if (process.argv.includes("--check")) {
    const r = check(root, { client });
    process.exitCode = r.missing.length ? 1 : 0;
  } else materialize(root, { client, from: arg("--from"), to: arg("--to") });
}
