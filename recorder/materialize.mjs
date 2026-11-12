// Materialize: rebuild the files that recorder/offload.mjs moved into release assets, so a checkout reads exactly
// as it would have if every run had committed its data to git (CHANGES 2026-11-12, 2026-11-12c).
//
//   node recorder/materialize.mjs <root> [--from YYYY-MM-DD] [--to YYYY-MM-DD]
//   node recorder/materialize.mjs <root> --check
//   node recorder/materialize.mjs <root> --adopt
//
// Reads the manifests in day order (manifest.mjs). For each bundle not yet applied it downloads the asset, checks
// its size and sha256, unpacks it and checks each file's sha256, then applies the entries:
// - a "file" entry is written, and must be absent or byte-identical;
// - an "append" entry goes at its offset in its bar day file. The file must end exactly there and the chunk before
//   it must be in place (the HEAD content, for the first chunk); a chunk already in place is skipped. So a run
//   applies each chunk once and only where it belongs, whatever .materialized.json says.
// Applied bundles are listed in .materialized.json (not tracked), so a second run only looks at what is new.
// --from/--to limit the days.
//
// --check applies nothing. It lists release assets that no manifest names, manifest lines whose asset is missing
// or at another size, and bar chunks parked in state/offload-retry (in git, not yet in any asset). Exit 1 if any
// line has no asset.
// --adopt writes the manifest line for each unlisted asset that carries one (manifest.json, from 2026-11-12c on)
// and fits: its files are not already in git, and its bar chunks start where their files now end. Anything else is
// reported and left alone. It changes manifests/ only; commit them from a fresh checkout of main while no record
// run is between its offload and its push (the record chain's bars offload also adopts a lost bar line by itself).
//
// Run materialize on a checkout you do not commit from: the rebuilt files are untracked or modified, by design.
import { execFileSync } from "node:child_process";
import { appendFileSync, closeSync, existsSync, mkdirSync, mkdtempSync, openSync, readFileSync, readSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { sha256 } from "./lib.mjs";
import { ASSET_RE, RELEASE_RE, appendLayout, headSize, logicalEnd, manifestLines, retryFiles } from "./manifest.mjs";
import { MANIFESTS, dayOfPath } from "./offload.mjs";

export { manifestLines };
const STATE = ".materialized.json";

// REST only (gh api): `gh release download` and `gh release list` go through GraphQL, which some research
// sessions cannot reach.
export function ghReader(repo) {
  const api = (path, args = []) => execFileSync("gh", ["api", ...args, path], { maxBuffer: 1 << 30, stdio: ["ignore", "pipe", "pipe"] });
  const release = (tag) => JSON.parse(api(`repos/${repo}/releases/tags/${tag}`).toString("utf8"));
  return {
    download(tag, name, dir) {
      const a = api(`repos/${repo}/releases/${release(tag).id}/assets?per_page=100`, ["--paginate", "--jq", ".[] | {id, name}"])
        .toString("utf8")
        .split("\n")
        .filter(Boolean)
        .map((l) => JSON.parse(l))
        .find((x) => x.name === name);
      if (!a) throw new Error(`${tag}/${name}: no such asset`);
      const p = join(dir, name);
      writeFileSync(p, api(`repos/${repo}/releases/assets/${a.id}`, ["-H", "Accept: application/octet-stream"]));
      return p;
    },
    releases() {
      return api(`repos/${repo}/releases?per_page=100`, ["--paginate", "--jq", ".[].tag_name"])
        .toString("utf8")
        .split("\n")
        .filter((t) => RELEASE_RE.test(t));
    },
    assets(tag) {
      // paged: a release's own asset list may be cut short on a busy day
      return api(`repos/${repo}/releases/${release(tag).id}/assets?per_page=100`, ["--paginate", "--jq", ".[] | {name, size, digest}"])
        .toString("utf8")
        .split("\n")
        .filter(Boolean)
        .map((l) => JSON.parse(l));
    },
  };
}

// Path entries come from a committed manifest, but are still checked: relative, under data/, no "..".
const safe = (p) => typeof p === "string" && p.startsWith("data/") && !p.split("/").includes("..") && !p.includes("\\");

const sizeOf = (p) => (existsSync(p) ? statSync(p).size : 0);
function slice(p, at, n) {
  const b = Buffer.alloc(n);
  const fd = openSync(p, "r");
  try {
    readSync(fd, b, 0, n, at);
  } finally {
    closeSync(fd);
  }
  return b;
}
const headBlob = (root, path) => execFileSync("git", ["-C", root, "show", `HEAD:${path}`], { maxBuffer: 1 << 30, stdio: ["ignore", "pipe", "ignore"] });

/** Download, check (against `expect`, when a manifest line gives it) and unpack one bundle; returns {dir, buf}. */
function fetchBundle(client, release, asset, work, expect) {
  if (!RELEASE_RE.test(release) || !ASSET_RE.test(asset)) throw new Error(`unexpected release or asset name: ${release}/${asset}`);
  const file = client.download(release, asset, work);
  const buf = readFileSync(file);
  if (expect && (buf.length !== expect.size || sha256(buf) !== expect.sha256)) throw new Error(`${asset}: size or sha256 does not match its manifest line`);
  const dir = join(work, "x");
  mkdirSync(dir);
  execFileSync("tar", ["-xzf", file, "-C", dir, "--no-same-owner", "--no-same-permissions"], { stdio: "ignore" });
  return { dir, buf };
}

export function materialize(root, { client, from, to, log = console.log } = {}) {
  const statePath = join(root, STATE);
  const state = existsSync(statePath) ? JSON.parse(readFileSync(statePath, "utf8")) : { applied: [] };
  const done = new Set(state.applied);
  const { lines } = appendLayout(root, manifestLines(root, { from, to }));
  // the chunk before each chunk of the same file, for the prefix check
  const prevOf = new Map();
  const last = new Map();
  for (const m of lines)
    for (const f of m.files)
      if (f.mode === "append") {
        prevOf.set(f, last.get(f.path) ?? null);
        last.set(f.path, f);
      }
  let applied = 0;
  let files = 0;
  let skipped = 0;
  for (const m of lines) {
    if (done.has(m.asset)) continue;
    const work = mkdtempSync(join(tmpdir(), "materialize-"));
    try {
      const { dir } = fetchBundle(client, m.release, m.asset, work, m);
      // check every entry before touching the checkout, so a bad bundle changes nothing
      const parts = m.files.map((f) => {
        if (!safe(f.path)) throw new Error(`${m.asset}: unsafe path ${f.path}`);
        if (f.mode !== "file" && f.mode !== "append") throw new Error(`${m.asset}: unknown mode ${f.mode}`);
        const bytes = readFileSync(join(dir, f.path));
        if (bytes.length !== f.bytes || sha256(bytes) !== f.sha256) throw new Error(`${m.asset}: ${f.path} does not match its manifest entry`);
        const target = join(root, f.path);
        if (f.mode === "file") {
          if (existsSync(target) && !readFileSync(target).equals(bytes)) throw new Error(`${m.asset}: ${f.path} already exists with other content`);
          return { ...f, target, data: bytes, write: !existsSync(target) };
        }
        const size = sizeOf(target);
        if (size >= f.offset + f.bytes && sha256(slice(target, f.offset, f.bytes)) === f.sha256) return { ...f, target, data: bytes, write: false }; // already in place
        // an append cut off part way: the start of this chunk is there, so finish it
        let rest = bytes;
        if (size > f.offset && size < f.offset + f.bytes && slice(target, f.offset, size - f.offset).equals(bytes.subarray(0, size - f.offset))) rest = bytes.subarray(size - f.offset);
        else if (size !== f.offset) throw new Error(`${m.asset}: ${f.path} ends at ${size}, but this chunk goes at ${f.offset}`);
        const prev = prevOf.get(f);
        if (prev && sha256(slice(target, prev.offset, prev.bytes)) !== prev.sha256) throw new Error(`${m.asset}: ${f.path} does not hold the chunk before this one`);
        if (!prev && f.offset > 0 && !slice(target, 0, f.offset).equals(headBlob(root, f.path))) throw new Error(`${m.asset}: ${f.path} does not start with its HEAD content`);
        return { ...f, target, data: rest, write: true };
      });
      for (const p of parts) {
        if (!p.write) {
          skipped++;
          continue;
        }
        mkdirSync(dirname(p.target), { recursive: true });
        if (p.mode === "file") writeFileSync(p.target, p.data);
        else appendFileSync(p.target, p.data);
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
  log(`materialized ${applied} bundles (${files} files written, ${skipped} already in place); ${done.size} applied in all`);
  return { applied, files, skipped };
}

export function check(root, { client, log = console.log } = {}) {
  const lines = manifestLines(root);
  const named = new Set(lines.map((m) => `${m.release}/${m.asset}`));
  const present = new Map();
  for (const tag of client.releases()) for (const a of client.assets(tag)) present.set(`${tag}/${a.name}`, a.size);
  const unlisted = [...present.keys()].filter((k) => !named.has(k)).sort();
  const missing = lines.filter((m) => present.get(`${m.release}/${m.asset}`) !== m.size).map((m) => `${m.release}/${m.asset}`);
  const parked = retryFiles(root).map((r) => r.path);
  log(`${lines.length} manifest lines, ${present.size} assets; ${unlisted.length} assets no manifest names, ${missing.length} lines with no asset at their size, ${parked.length} bar chunks parked`);
  for (const k of unlisted) log(`unlisted: ${k}`);
  for (const k of missing) log(`missing: ${k}`);
  for (const k of parked) log(`parked: ${k}`);
  return { unlisted, missing, parked };
}

export function adopt(root, { client, log = console.log } = {}) {
  const { unlisted } = check(root, { client, log: () => {} });
  const adopted = [];
  const refused = [];
  const parked = new Set(retryFiles(root).map((r) => r.path));
  for (const key of unlisted) {
    const [release, asset] = key.split("/");
    const work = mkdtempSync(join(tmpdir(), "adopt-"));
    try {
      const { dir, buf } = fetchBundle(client, release, asset, work);
      if (!existsSync(join(dir, "manifest.json"))) throw new Error("carries no manifest line (written before 2026-11-12c)");
      const head = JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8"));
      if (head.asset !== asset || head.release !== release || !MANIFESTS[head.kind] || !asset.startsWith(`${head.kind}-`)) throw new Error("its manifest line names another asset or kind");
      const day = release.slice(5);
      for (const f of head.files) {
        if (!safe(f.path) || dayOfPath(f.path) !== day) throw new Error(`unsafe or misplaced path ${f.path}`);
        const bytes = readFileSync(join(dir, f.path));
        if (bytes.length !== f.bytes || sha256(bytes) !== f.sha256) throw new Error(`${f.path} does not match its entry`);
        if (f.mode === "file" && existsSync(join(root, f.path))) throw new Error(`${f.path} is already in the checkout`);
        if (f.mode === "append") {
          if (parked.has(f.path)) throw new Error(`${f.path} has a parked chunk, which holds these rows`);
          const end = logicalEnd(root, f.path, day);
          if (f.offset !== end) throw new Error(`${f.path} chunk at ${f.offset}, but the file now ends at ${end}`);
        }
      }
      const line = { ...head, size: buf.length, sha256: sha256(buf) };
      const mf = join(root, MANIFESTS[head.kind], `${day}.ndjson`);
      mkdirSync(dirname(mf), { recursive: true });
      appendFileSync(mf, JSON.stringify(line) + "\n");
      adopted.push(key);
      log(`adopted: ${key}`);
    } catch (e) {
      refused.push(key);
      log(`not adopted: ${key}: ${e.message}`);
    } finally {
      rmSync(work, { recursive: true, force: true });
    }
  }
  return { adopted, refused };
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
  } else if (process.argv.includes("--adopt")) adopt(root, { client });
  else materialize(root, { client, from: arg("--from"), to: arg("--to") });
}
