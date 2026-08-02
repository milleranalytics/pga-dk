// Differential test for src/optimizer.ts against exhaustive search.
//
// The invariant under test, stated as a property rather than a sample:
//   for every instance, solve() returns exactly K DISTINCT players, total
//   salary <= cap, and objective == the exhaustive maximum.
//
// Run: node test/optimizer-check.mjs
//
// This exists because the design prototype's optimizer looked correct, produced
// plausible lineups, and rostered the same player twice in 34% of solves
// (test/dp-check.mjs). Passing eyeball inspection is not evidence here.

import { readFileSync } from "node:fs";

// solve() is plain TS with no type-only runtime deps, so strip the annotations
// rather than pulling in a build step for one test.
const src = readFileSync(new URL("../src/optimizer.ts", import.meta.url), "utf8");
const jsBody = src
  .replace(/export interface[\s\S]*?\n}\n/g, "")
  .replace(/: OptPlayer\[\] \| null/g, "")
  .replace(/: OptPlayer\[\]/g, "")
  .replace(/: BuildContext/g, "")
  .replace(/: Set<string>/g, "")
  .replace(/: string\[\]\[\]/g, "")
  .replace(/: string\[\]/g, "")
  .replace(/: Map<string, number>/g, "")
  .replace(/: number\[\]/g, "")
  .replace(/: number/g, "")
  .replace(/: string/g, "")
  .replace(/ as OptPlayer\[\]/g, "")
  .replace(/new Set<string>/g, "new Set")
  .replace(/new Map<string, number>/g, "new Map")
  .replace(/^export /gm, "");
const mod = await import(
  "data:text/javascript;base64," +
    Buffer.from(jsBody + "\nexport { solve, optimize, generate };").toString("base64")
);
const { solve } = mod;

function brute(pool, K, cap) {
  let best = -Infinity;
  let bestSet = null;
  const idx = [];
  (function rec(start, chosen, sal, val) {
    if (chosen === K) {
      if (sal <= cap && val > best) { best = val; bestSet = idx.slice(); }
      return;
    }
    for (let i = start; i < pool.length; i++) {
      if (sal + pool[i].salary > cap) continue;
      idx.push(i);
      rec(i + 1, chosen + 1, sal + pool[i].salary, val + pool[i].value);
      idx.pop();
    }
  })(0, 0, 0, 0);
  return { value: best, set: bestSet };
}

let dupes = 0, subopt = 0, overCap = 0, wrongSize = 0, feasible = 0, mismatchNull = 0;

const TRIALS = 4000;
for (let t = 0; t < TRIALS; t++) {
  const n = 7 + Math.floor(Math.random() * 6);
  const K = 2 + Math.floor(Math.random() * 5);
  // Tight caps on purpose — that is where reconstruction bugs surface.
  const cap = (200 + Math.floor(Math.random() * 350)) * 100;
  const pool = Array.from({ length: n }, (_, i) => ({
    id: "p" + i,
    salary: (60 + Math.floor(Math.random() * 46)) * 100,
    value: Math.round(Math.random() * 5000) / 10000,
  }));

  const got = solve(pool, K, cap);
  const want = brute(pool, K, cap);

  if (want.set === null) {
    if (got !== null) mismatchNull++;   // claimed a lineup where none exists
    continue;
  }
  feasible++;
  if (got === null) { mismatchNull++; continue; }

  const ids = got.map((p) => p.id);
  if (new Set(ids).size !== ids.length) dupes++;
  if (ids.length !== K) wrongSize++;
  const sal = got.reduce((a, p) => a + p.salary, 0);
  if (sal > cap) overCap++;
  const val = got.reduce((a, p) => a + p.value, 0);
  if (Math.abs(val - want.value) > 1e-9) subopt++;
}

const fails = dupes + subopt + overCap + wrongSize + mismatchNull;
console.log(`instances with a feasible lineup: ${feasible} / ${TRIALS}`);
console.log(`  duplicate player in result:     ${dupes}`);
console.log(`  wrong roster size:              ${wrongSize}`);
console.log(`  over the salary cap:            ${overCap}`);
console.log(`  objective != exhaustive max:    ${subopt}`);
console.log(`  feasibility disagreement:       ${mismatchNull}`);
console.log(fails === 0 ? "\nPASS — invariant holds on every instance." : `\nFAIL — ${fails} violation(s).`);
process.exit(fails === 0 ? 0 : 1);
