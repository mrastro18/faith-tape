// Copied unchanged from FAITH (github.com/mrastro18/faith) scripts/loop/stats.mjs at commit b801165a0eec3203a7471eab7f60ac11d3a0bc44
// (branch claude/queue-sim). Only these two header lines are added. Do not edit here: change FAITH and copy again.
// Statistics for the scorer. Deterministic: every random draw comes from a seeded generator.

export const mean = (xs) => (xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : NaN);

export function sd(xs) {
  if (xs.length < 2) return NaN;
  const m = mean(xs);
  return Math.sqrt(xs.reduce((a, b) => a + (b - m) ** 2, 0) / (xs.length - 1));
}

export function quantile(sorted, q) {
  if (!sorted.length) return NaN;
  const pos = (sorted.length - 1) * q;
  const lo = Math.floor(pos);
  const hi = Math.ceil(pos);
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo);
}

/** mulberry32: small, fast, reproducible. */
export function rng(seed) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// Standard normal CDF (Abramowitz-Stegun 7.1.26 via erf, |error| < 1.5e-7).
export function normCdf(x) {
  const z = Math.abs(x) / Math.SQRT2;
  const t = 1 / (1 + 0.3275911 * z);
  const y =
    1 -
    ((((1.061405429 * t - 1.453152027) * t + 1.421413741) * t - 0.284496736) * t + 0.254829592) *
      t *
      Math.exp(-z * z);
  return x >= 0 ? 0.5 * (1 + y) : 0.5 * (1 - y);
}

/** Inverse normal CDF (Acklam), relative error under 1.2e-9. */
export function normInv(p) {
  if (!(p > 0 && p < 1)) throw new Error(`normInv: p=${p}`);
  const a = [
    -3.969683028665376e1, 2.209460984245205e2, -2.759285104469687e2, 1.38357751867269e2,
    -3.066479806614716e1, 2.506628277459239,
  ];
  const b = [
    -5.447609879822406e1, 1.615858368580409e2, -1.556989798598866e2, 6.680131188771972e1,
    -1.328068155288572e1,
  ];
  const c = [
    -7.784894002430293e-3, -3.223964580411365e-1, -2.400758277161838, -2.549732539343734,
    4.374664141464968, 2.938163982698783,
  ];
  const d = [7.784695709041462e-3, 3.224671290700398e-1, 2.445134137142996, 3.754408661907416];
  const pl = 0.02425;
  let q;
  if (p < pl) {
    q = Math.sqrt(-2 * Math.log(p));
    return (
      (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) /
      ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    );
  }
  if (p > 1 - pl) {
    q = Math.sqrt(-2 * Math.log(1 - p));
    return (
      -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) /
      ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    );
  }
  q = p - 0.5;
  const r = q * q;
  return (
    ((((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q) /
    (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)
  );
}

// Log-gamma (Lanczos, g = 7), for the t distribution below.
function lgamma(x) {
  const c = [
    0.99999999999980993, 676.5203681218851, -1259.1392167224028, 771.32342877765313,
    -176.61502916214059, 12.507343278686905, -0.13857109526572012, 9.9843695780195716e-6,
    1.5056327351493116e-7,
  ];
  if (x < 0.5) return Math.log(Math.PI / Math.abs(Math.sin(Math.PI * x))) - lgamma(1 - x);
  x -= 1;
  let a = c[0];
  const t = x + 7.5;
  for (let i = 1; i < 9; i++) a += c[i] / (x + i);
  return 0.5 * Math.log(2 * Math.PI) + (x + 0.5) * Math.log(t) - t + Math.log(a);
}

// Regularised incomplete beta I_x(a, b), by Lentz's continued fraction.
function betaInc(x, a, b) {
  if (x <= 0) return 0;
  if (x >= 1) return 1;
  const front = Math.exp(
    lgamma(a + b) - lgamma(a) - lgamma(b) + a * Math.log(x) + b * Math.log(1 - x),
  );
  if (x > (a + 1) / (a + b + 2)) return 1 - betaInc(1 - x, b, a);
  let f = 1;
  let C = 1;
  let D = 0;
  for (let i = 0; i <= 300; i++) {
    const m = i >> 1;
    let num;
    if (i === 0) num = 1;
    else if (i % 2 === 0) num = (m * (b - m) * x) / ((a + 2 * m - 1) * (a + 2 * m));
    else num = -((a + m) * (a + b + m) * x) / ((a + 2 * m) * (a + 2 * m + 1));
    D = 1 + num * D;
    if (Math.abs(D) < 1e-300) D = 1e-300;
    D = 1 / D;
    C = 1 + num / C;
    if (Math.abs(C) < 1e-300) C = 1e-300;
    const cd = C * D;
    f *= cd;
    if (Math.abs(1 - cd) < 1e-14) break;
  }
  return (front * (f - 1)) / a;
}

/** Student t CDF with `df` degrees of freedom. */
export function tCdf(t, df) {
  if (!(df > 0)) return NaN;
  const x = df / (df + t * t);
  const tail = 0.5 * betaInc(x, df / 2, 0.5);
  return t >= 0 ? 1 - tail : tail;
}

/** Inverse Student t CDF, by bisection on tCdf. */
export function tInv(p, df) {
  if (!(p > 0 && p < 1)) throw new Error(`tInv: p=${p}`);
  let lo = -1e3;
  let hi = 1e3;
  for (let i = 0; i < 200; i++) {
    const mid = (lo + hi) / 2;
    if (tCdf(mid, df) < p) lo = mid;
    else hi = mid;
  }
  return (lo + hi) / 2;
}

export const MIN_CLUSTERS = 30; // 07 section 8, item 1: inference needs at least 30 independent days

/**
 * Inference on a (weighted) mean whose items share clusters (days, by default): trades on one day share one
 * market and are not independent (07 section 8, item 1). `items` are {value, cluster, weight?}; the weight
 * is the notional, so a bid swept in full counts for its size, not as one row among many small ones.
 * Two references, and the more cautious of the two is reported (second review, item 9):
 *   - a cluster-robust standard error with a t reference on clusters - 1 degrees of freedom;
 *   - a studentised (bootstrap-t) cluster bootstrap, which corrects for the skew of resting-bid payoffs
 *     (many small gains, a few large losses), where the plain t reference is too generous.
 * p is the larger p-value and lo the lower of the two lower bounds (hi the higher upper bound). Below
 * `minClusters` the mean is reported and nothing is inferred: p, lo and hi are NaN.
 */
export function clusterBootstrap(
  items,
  { resamples = 2000, level = 0.9, seed = 20261110, minClusters = MIN_CLUSTERS } = {},
) {
  const groups = new Map();
  for (const it of items) {
    const w = it.weight ?? 1;
    if (!(w >= 0) || !Number.isFinite(it.value))
      throw new Error("clusterBootstrap: values must be finite and weights non-negative");
    if (!groups.has(it.cluster)) groups.set(it.cluster, { sw: 0, swv: 0 });
    const c = groups.get(it.cluster);
    c.sw += w;
    c.swv += w * it.value;
  }
  const g = [...groups.values()];
  const G = g.length;
  const stat = (idx) => {
    let W = 0;
    let S = 0;
    for (const i of idx) {
      W += g[i].sw;
      S += g[i].swv;
    }
    if (!(W > 0)) return { m: NaN, se: NaN };
    const m = S / W;
    let ss = 0;
    for (const i of idx) ss += (g[i].swv - m * g[i].sw) ** 2;
    return { m, se: idx.length > 1 ? Math.sqrt((idx.length / (idx.length - 1)) * ss) / W : NaN };
  };
  const all = g.map((_, i) => i);
  const { m: point, se } = stat(all);
  const out = { mean: point, lo: NaN, hi: NaN, p: NaN, se, clusters: G, n: items.length };
  if (G < minClusters || !Number.isFinite(point)) return out;
  const tail = (1 - level) / 2;
  if (!(se > 0)) {
    out.lo = out.hi = point;
    out.p = point > 0 ? 0 : 1;
    return out;
  }
  const df = G - 1;
  const t0 = point / se;
  const tq = tInv(1 - tail, df);
  const pT = 1 - tCdf(t0, df);
  // Bootstrap-t: resample whole clusters, studentise each resample around the point estimate.
  const r = rng(seed);
  const ts = [];
  let atLeast = 0;
  const idx = new Array(G);
  for (let k = 0; k < resamples; k++) {
    for (let j = 0; j < G; j++) idx[j] = Math.floor(r() * G);
    const s = stat(idx);
    if (!(s.se > 0)) continue;
    const tk = (s.m - point) / s.se;
    ts.push(tk);
    if (tk >= t0) atLeast++;
  }
  ts.sort((x, y) => x - y);
  const pBT = (atLeast + 1) / (ts.length + 1);
  const loBT = point - quantile(ts, 1 - tail) * se;
  const hiBT = point - quantile(ts, tail) * se;
  out.p = Math.max(pT, pBT);
  out.lo = Math.min(point - tq * se, loBT);
  out.hi = Math.max(point + tq * se, hiBT);
  out.ref = { t: { p: pT, lo: point - tq * se }, bootT: { p: pBT, lo: loBT } };
  return out;
}

/** Power of a one-sided z-test at level alpha for a true mean `effect`, per-unit sd `s`, and n units. */
export function power(effect, s, n, alpha) {
  if (!(s > 0) || !(n > 0)) return 0;
  return 1 - normCdf(normInv(1 - alpha) - (effect / s) * Math.sqrt(n));
}

/**
 * O'Brien-Fleming boundaries for a one-sided group-sequential test with looks at information fractions
 * `t` (ascending, last = 1). Boundary at look k is C / sqrt(t_k); C is found so the overall type I error
 * is `alpha`, by recursive numerical integration of the Brownian path (exact up to the grid step).
 */
export function obrienFleming(t, alpha = 0.05) {
  if (!t.length || Math.abs(t[t.length - 1] - 1) > 1e-12)
    throw new Error("obrienFleming: last fraction must be 1");
  const err = (C) =>
    crossProb(
      t,
      t.map((tk) => C / Math.sqrt(tk)),
    );
  let lo = 0.5;
  let hi = 8;
  for (let i = 0; i < 60; i++) {
    const mid = (lo + hi) / 2;
    if (err(mid) > alpha) lo = mid;
    else hi = mid;
  }
  const C = (lo + hi) / 2;
  return { C, z: t.map((tk) => C / Math.sqrt(tk)), alpha: err(C) };
}

/** Probability that the standardised statistic crosses z_k at some look, under zero drift. */
export function crossProb(t, z, h = 0.01) {
  // Track the density of S = W(t) on a grid, for paths that have not crossed yet.
  let grid = null; // {x0, dens: Float64Array}
  let total = 0;
  let prevT = 0;
  for (let k = 0; k < t.length; k++) {
    const dt = t[k] - prevT;
    const sdt = Math.sqrt(dt);
    const bound = z[k] * Math.sqrt(t[k]); // boundary on the S scale
    if (!grid) {
      total += 1 - normCdf(bound / sdt);
      const x0 = -8 * sdt;
      const n = Math.max(1, Math.ceil((bound - x0) / h));
      const dens = new Float64Array(n);
      for (let i = 0; i < n; i++) {
        const x = x0 + (i + 0.5) * h;
        dens[i] = (Math.exp((-x * x) / (2 * dt)) / (sdt * Math.sqrt(2 * Math.PI))) * h;
      }
      grid = { x0, dens };
    } else {
      // Mass crossing now, and the new density below the boundary.
      let cross = 0;
      for (let i = 0; i < grid.dens.length; i++) {
        const x = grid.x0 + (i + 0.5) * h;
        cross += grid.dens[i] * (1 - normCdf((bound - x) / sdt));
      }
      total += cross;
      if (k < t.length - 1) {
        const x0 = grid.x0 - 8 * sdt;
        const n = Math.max(1, Math.ceil((bound - x0) / h));
        const dens = new Float64Array(n);
        const kern = (d) => Math.exp((-d * d) / (2 * dt)) / (sdt * Math.sqrt(2 * Math.PI));
        for (let i = 0; i < grid.dens.length; i++) {
          const m = grid.dens[i];
          if (m < 1e-14) continue;
          const x = grid.x0 + (i + 0.5) * h;
          const jlo = Math.max(0, Math.floor((x - 8 * sdt - x0) / h));
          const jhi = Math.min(n, Math.ceil((x + 8 * sdt - x0) / h));
          for (let j = jlo; j < jhi; j++) dens[j] += m * kern(x0 + (j + 0.5) * h - x) * h;
        }
        grid = { x0, dens };
      }
    }
    prevT = t[k];
  }
  return total;
}

/**
 * Benjamini-Hochberg at level q over a fixed family. `family` lists every member's id in the order fixed
 * at registration; members without a p-value yet count as not rejected and still count toward m, so an
 * untested member can never make the bar easier.
 */
export function benjaminiHochberg(family, pvals, q) {
  const m = family.length;
  const tested = family
    .filter((id) => Number.isFinite(pvals[id]))
    .map((id) => ({ id, p: pvals[id] }));
  tested.sort((a, b) => a.p - b.p);
  let kMax = 0;
  tested.forEach((x, i) => {
    if (x.p <= ((i + 1) / m) * q) kMax = i + 1;
  });
  const rejected = new Set(tested.slice(0, kMax).map((x) => x.id));
  return Object.fromEntries(family.map((id) => [id, rejected.has(id)]));
}
