// Differential test for src/optimizer.ts against exhaustive search.
//
// The invariant under test, stated as a property rather than a sample:
//   for every instance, solve() returns exactly K DISTINCT players, total
//   salary <= cap, and objective == the exhaustive maximum.
//
// Run: npm run test:optimizer
//
// This exists because the design prototype's optimizer looked correct, produced
// plausible lineups, and rostered the same player twice in 34% of solves
// (test/dp-check.mjs). Passing eyeball inspection is not evidence here.
//
// Bundled through esbuild rather than importing the .ts directly. An earlier
// version stripped the types with hand-written regexes and broke the moment git
// rewrote the working copy with CRLF line endings, because the patterns looked
// for "\n}". Let the bundler parse TypeScript.

import { solve } from "../src/optimizer";
import type { OptPlayer } from "../src/optimizer";

function brute(pool: OptPlayer[], K: number, cap: number) {
  let best = -Infinity;
  let bestSet: number[] | null = null;
  const idx: number[] = [];
  (function rec(start: number, chosen: number, sal: number, val: number) {
    if (chosen === K) {
      if (sal <= cap && val > best) {
        best = val;
        bestSet = idx.slice();
      }
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

let dupes = 0;
let subopt = 0;
let overCap = 0;
let wrongSize = 0;
let feasible = 0;
let mismatchNull = 0;

const TRIALS = 4000;
for (let t = 0; t < TRIALS; t++) {
  const n = 7 + Math.floor(Math.random() * 6);
  const K = 2 + Math.floor(Math.random() * 5);
  // Tight caps on purpose — that is where reconstruction bugs surface.
  const cap = (200 + Math.floor(Math.random() * 350)) * 100;
  const pool: OptPlayer[] = Array.from({ length: n }, (_, i) => ({
    id: "p" + i,
    salary: (60 + Math.floor(Math.random() * 46)) * 100,
    value: Math.round(Math.random() * 5000) / 10000,
  }));

  const got = solve(pool, K, cap);
  const want = brute(pool, K, cap);

  if (want.set === null) {
    if (got !== null) mismatchNull++; // claimed a lineup where none exists
    continue;
  }
  feasible++;
  if (got === null) {
    mismatchNull++;
    continue;
  }

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
console.log(
  fails === 0 ? "\nPASS — invariant holds on every instance." : `\nFAIL — ${fails} violation(s).`,
);
process.exit(fails === 0 ? 0 : 1);
