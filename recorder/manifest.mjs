// Manifest reading and the byte layout of one-second bar day files, shared by offload.mjs and materialize.mjs
// (CHANGES 2026-11-12c).
//
// A bar day file is rebuilt as: its content at HEAD (only files that were in git before the offload switch have
// any), then every "append" chunk the manifests list for it, in manifest order. Each chunk's place in that file is
// its offset. Lines written from 2026-11-12c on carry it; older lines get the offset implied by the same rule,
// which holds because no bar file changed in git after the switch (checked 2026-11-12 05:20 UTC) and, from this
// change on, a failed offload parks its chunk in state/offload-retry instead of committing it at its path.
import { execFileSync } from "node:child_process";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";

// manifests/<day>.ndjson (record chain: quotes and bars, in run order), then manifests/bridge/<day>.ndjson (bridge
// quotes, whole files only, so their order against the chain's lines does not matter). Within a day the chain's
// file is read first, always.
export const MANIFEST_DIRS = ["manifests", "manifests/bridge"];
export const RETRY = "state/offload-retry";

export const RELEASE_RE = /^tape-\d{4}-\d{2}-\d{2}$/;
export const ASSET_RE = /^(quotes|bars|bridge)-\d{4}-\d{2}-\d{2}-[A-Za-z0-9_-]+\.tar\.gz$/;

export function manifestLines(root, { from, to } = {}) {
  const byDay = new Map();
  for (const sub of MANIFEST_DIRS) {
    const dir = join(root, sub);
    if (!existsSync(dir)) continue;
    for (const f of readdirSync(dir).sort()) {
      if (!/^\d{4}-\d{2}-\d{2}\.ndjson$/.test(f)) continue;
      const d = f.slice(0, 10);
      if ((from && d < from) || (to && d > to)) continue;
      if (!byDay.has(d)) byDay.set(d, []);
      byDay.get(d).push(join(dir, f));
    }
  }
  const out = [];
  for (const d of [...byDay.keys()].sort())
    for (const file of byDay.get(d))
      for (const l of readFileSync(file, "utf8").split("\n")) if (l.trim()) out.push({ day: d, ...JSON.parse(l) });
  return out;
}

/** Sizes of every blob at HEAD under `dirs` (one git call), as a Map path -> bytes. */
export function headSizes(root, dirs) {
  const out = new Map();
  const ex = dirs.filter((d) => d);
  if (!ex.length) return out;
  let txt = "";
  try {
    txt = execFileSync("git", ["-C", root, "ls-tree", "-r", "-l", "-z", "HEAD", "--", ...ex], { encoding: "utf8", maxBuffer: 1 << 30, stdio: ["ignore", "pipe", "ignore"] });
  } catch {
    return out;
  }
  for (const rec of txt.split("\0")) {
    const tab = rec.indexOf("\t");
    if (tab < 0) continue;
    out.set(rec.slice(tab + 1), Number(rec.slice(0, tab).trim().split(/\s+/)[3]));
  }
  return out;
}

/** Size of a path's blob at HEAD, 0 if it is not tracked. */
export function headSize(root, path) {
  try {
    const out = execFileSync("git", ["-C", root, "ls-tree", "-l", "HEAD", "--", path], { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim();
    if (!out) return 0;
    return Number(out.split(/\s+/)[3]);
  } catch {
    return 0;
  }
}

/**
 * Give every append entry its offset (recorded, or implied for older lines) and return {lines, ends}: `ends` maps a
 * path to the size its file has once every listed chunk is in place. A recorded offset that disagrees with the
 * implied one is an error: the manifests no longer describe one file.
 */
export function appendLayout(root, lines, sizes) {
  if (!sizes) {
    const dirs = new Set();
    for (const m of lines) for (const f of m.files ?? []) if (f.mode === "append") dirs.add(f.path.split("/").slice(0, 3).join("/"));
    sizes = headSizes(root, [...dirs]);
  }
  const ends = new Map();
  for (const m of lines)
    for (const f of m.files ?? []) {
      if (f.mode !== "append") continue;
      if (!ends.has(f.path)) ends.set(f.path, sizes.get(f.path) ?? 0);
      const at = ends.get(f.path);
      if (f.offset !== undefined && f.offset !== at) throw new Error(`${m.asset}: ${f.path} chunk at offset ${f.offset}, but the chunks before it end at ${at}`);
      f.offset = at;
      ends.set(f.path, at + f.bytes);
    }
  return { lines, ends };
}

/** Logical ends of a day's bar files: a function path -> HEAD size plus every chunk the day's manifest lists. */
export function dayEnds(root, day, dirs) {
  const sizes = headSizes(root, dirs);
  const { ends } = appendLayout(root, manifestLines(root, { from: day, to: day }), sizes);
  return (path) => ends.get(path) ?? sizes.get(path) ?? 0;
}

/** Logical end of one bar day file (one-off use; dayEnds for many paths). */
export function logicalEnd(root, path, day) {
  return dayEnds(root, day, [path.split("/").slice(0, 3).join("/")])(path);
}

/** Files parked by a failed bars offload: [{path (the data path it belongs to), file (its place under RETRY)}]. */
export function retryFiles(root) {
  const base = join(root, RETRY);
  if (!existsSync(base)) return [];
  const out = [];
  const walk = (rel) => {
    for (const e of readdirSync(join(base, rel), { withFileTypes: true })) {
      const r = rel ? `${rel}/${e.name}` : e.name;
      if (e.isDirectory()) walk(r);
      else if (!e.name.endsWith(".tmp")) out.push({ path: r, file: `${RETRY}/${r}` });
    }
  };
  walk("");
  return out.sort((a, b) => (a.path < b.path ? -1 : 1));
}
