// Differential test for src/optimizer.ts against exhaustive search.
//
// The invariant under test, stated as a property rather than a sample:
//   for every instance, solve() returns exactly K DISTINCT players, total
//   salary <= cap, objective == the exhaustive maximum, and — among lineups
//   that tie on the objective — the cheapest one. The cap is a ceiling, never
//   a target.
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
  let bestSal = Infinity;
  let bestSet: number[] | null = null;
  const idx: number[] = [];
  (function rec(start: number, chosen: number, sal: number, val: number) {
    if (chosen === K) {
      // Cheapest of the equally-good lineups, so the tie-break below is a
      // property of the problem and not of enumeration order.
      if (sal <= cap && (val > best || (val === best && sal < bestSal))) {
        best = val;
        bestSal = sal;
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
  return { value: best, salary: bestSal, set: bestSet };
}

let dupes = 0;
let subopt = 0;
let overCap = 0;
let wrongSize = 0;
let feasible = 0;
let mismatchNull = 0;
let overspent = 0;

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
  // The cap is a CEILING, not a target: among lineups of equal objective the
  // cheapest is returned, so a full spend always means the money bought
  // something. (The DP scans the salary axis upward and keeps a bucket only on
  // a strict improvement, so this is structural — this asserts it stays that
  // way.) Real slates spend the whole cap anyway, because on a real slate more
  // salary does buy more P(top-20) — which is exactly why the property has to
  // be tested on instances where value and salary are UNCORRELATED, as these
  // random pools are.
  if (sal > want.salary) overspent++;
}

const fails = dupes + subopt + overCap + wrongSize + mismatchNull + overspent;
console.log(`instances with a feasible lineup: ${feasible} / ${TRIALS}`);
console.log(`  duplicate player in result:     ${dupes}`);
console.log(`  wrong roster size:              ${wrongSize}`);
console.log(`  over the salary cap:            ${overCap}`);
console.log(`  objective != exhaustive max:    ${subopt}`);
console.log(`  spent more than it had to:      ${overspent}`);
console.log(`  feasibility disagreement:       ${mismatchNull}`);
console.log(
  fails === 0 ? "\nPASS — invariant holds on every instance." : `\nFAIL — ${fails} violation(s).`,
);
process.exit(fails === 0 ? 0 : 1);
