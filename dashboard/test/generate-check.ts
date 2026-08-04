// Property test for generate() in src/optimizer.ts.
//
// The invariant under test, stated as a property rather than a sample:
//   generate(ctx, n, existing, pct) returns EXACTLY n lineups whenever n such
//   lineups exist, and every returned lineup is
//     - a full roster of distinct players under the cap,
//     - distinct from the others and from `existing`,
//     - a superset of the locks and disjoint from the exclusions,
//     - within the exposure ceiling across existing + returned.
//   When it returns fewer, `stop` names the constraint that ran out, and for
//   "exhausted" that claim is checked against exhaustive enumeration.
//
// Run: npm run test:generate
//
// This exists because the shipped version returned ONE lineup for Gen 5 on an
// empty saved list, and looked fine doing it — it re-solved a jittered
// objective, got the same optimum back every time, and silently dropped it as a
// duplicate. Nothing in the code said "return n distinct lineups" out loud, so
// nothing could notice it wasn't. This says it out loud.

import { generate, solve } from "../src/optimizer";
import type { OptPlayer, BuildContext, GenResult } from "../src/optimizer";

let failures = 0;
function check(label: string, ok: boolean, detail = "") {
  if (!ok) {
    failures++;
    console.log(`  FAIL  ${label}${detail ? " — " + detail : ""}`);
  }
}

// Deterministic pool generator — same instances every run.
function mulberry(seed: number) {
  return function () {
    seed |= 0;
    seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function pool(rand: () => number, n: number): OptPlayer[] {
  return Array.from({ length: n }, (_, i) => ({
    id: `p${i}`,
    // Multiples of 100, as DraftKings salaries are.
    salary: 4000 + Math.floor(rand() * 70) * 100,
    value: Math.round(rand() * 1000) / 1000,
  }));
}

/** Every feasible full roster, as sorted keys. Only for small pools. */
function allLineups(ctx: BuildContext): Set<string> {
  const out = new Set<string>();
  const usable = ctx.all.filter((p) => !ctx.excludedIds.has(p.id) || ctx.lockedIds.has(p.id));
  const chosen: OptPlayer[] = [];
  (function rec(start: number, sal: number) {
    if (chosen.length === ctx.slots) {
      if (sal <= ctx.cap && [...ctx.lockedIds].every((id) => chosen.some((p) => p.id === id))) {
        out.add(chosen.map((p) => p.id).sort().join("|"));
      }
      return;
    }
    for (let i = start; i < usable.length; i++) {
      if (sal + usable[i].salary > ctx.cap) continue;
      chosen.push(usable[i]);
      rec(i + 1, sal + usable[i].salary);
      chosen.pop();
    }
  })(0, 0);
  return out;
}

function audit(
  label: string,
  ctx: BuildContext,
  n: number,
  existing: string[][],
  pct: number,
  r: GenResult,
  exhaustive: Set<string> | null,
) {
  const byId = new Map(ctx.all.map((p) => [p.id, p]));
  const keys = r.lineups.map((ids) => [...ids].sort().join("|"));
  const existingKeys = existing.map((ids) => [...ids].sort().join("|"));

  for (const ids of r.lineups) {
    check(label, new Set(ids).size === ctx.slots, `roster of ${new Set(ids).size}, want ${ctx.slots}`);
    const sal = ids.reduce((a, id) => a + (byId.get(id)?.salary ?? 0), 0);
    check(label, sal <= ctx.cap, `salary ${sal} over cap ${ctx.cap}`);
    check(label, [...ctx.lockedIds].every((id) => ids.includes(id)), "lock missing");
    check(label, ids.every((id) => !ctx.excludedIds.has(id)), "excluded player used");
  }
  check(label, new Set(keys).size === keys.length, "duplicate lineups returned");
  check(label, keys.every((k) => !existingKeys.includes(k)), "returned an existing lineup");

  // Exposure ceiling, across existing + returned. Locks are exempt by design.
  const usage = new Map<string, number>();
  for (const ids of [...existing, ...r.lineups]) {
    for (const id of ids) usage.set(id, (usage.get(id) ?? 0) + 1);
  }
  const ceiling = Math.max(1, Math.floor((pct / 100) * (existing.length + n)));
  check(label, r.ceiling === ceiling, `ceiling ${r.ceiling}, want ${ceiling}`);
  for (const [id, u] of usage) {
    if (ctx.lockedIds.has(id)) continue;
    check(label, u <= ceiling, `${id} used ${u} > ceiling ${ceiling}`);
  }

  check(label, r.lineups.length <= n, `returned ${r.lineups.length} > asked ${n}`);
  check(
    label,
    (r.lineups.length === n) === (r.stop === "complete"),
    `stop=${r.stop} with ${r.lineups.length}/${n}`,
  );

  // The claim "no different roster is left to build" is checkable.
  if (exhaustive && r.stop === "exhausted") {
    const untouched = [...exhaustive].filter(
      (k) => !keys.includes(k) && !existingKeys.includes(k),
    );
    // Any roster left over must have been blocked by exposure, not missed.
    const blocked = untouched.every((k) =>
      k.split("|").some((id) => !ctx.lockedIds.has(id) && (usage.get(id) ?? 0) >= ceiling),
    );
    check(label, blocked, `${untouched.length} feasible rosters left unbuilt`);
  }
}

// --- 1. small pools, exhaustively cross-checked ----------------------------
{
  console.log("small pools vs exhaustive enumeration");
  for (let t = 0; t < 120; t++) {
    const rand = mulberry(t + 1);
    const all = pool(rand, 8 + Math.floor(rand() * 6));
    const slots = 2 + Math.floor(rand() * 3);
    const cap = 12000 + Math.floor(rand() * 20000);
    const ctx: BuildContext = {
      all,
      lockedIds: new Set(rand() < 0.3 ? [all[0].id] : []),
      excludedIds: new Set(rand() < 0.3 ? [all[all.length - 1].id] : []),
      pickedIds: [],
      slots,
      cap,
    };
    const n = 1 + Math.floor(rand() * 5);
    const feasible = allLineups(ctx);
    const r = generate(ctx, n, [], 100);
    audit(`instance ${t}`, ctx, n, [], 100, r, feasible);
    // With no exposure limit, n lineups must come back whenever n exist.
    check(
      `instance ${t}`,
      r.lineups.length === Math.min(n, feasible.size),
      `got ${r.lineups.length}, ${feasible.size} rosters feasible, asked ${n}`,
    );
  }
}

// --- 2. repeated presses on a realistic slate ------------------------------
{
  console.log("repeated Gen presses, 149-player slate");
  const rand = mulberry(99);
  const all = pool(rand, 149);
  const ctx: BuildContext = {
    all,
    lockedIds: new Set(),
    excludedIds: new Set(),
    pickedIds: [],
    slots: 6,
    cap: 50000,
  };
  let existing: string[][] = [];
  for (let press = 1; press <= 4; press++) {
    const r = generate(ctx, 5, existing, 60);
    audit(`press ${press}`, ctx, 5, existing, 60, r, null);
    check(`press ${press}`, r.lineups.length === 5, `added ${r.lineups.length} (${r.stop})`);
    existing = [...existing, ...r.lineups];
  }
  check("4 presses", existing.length === 20, `${existing.length} lineups`);

  // Pre-committed picks must NOT leak into Gen: it builds fresh rosters.
  const withPicks = generate({ ...ctx, pickedIds: [all[3].id] }, 5, [], 60);
  const clean = generate(ctx, 5, [], 60);
  check(
    "picks ignored",
    JSON.stringify(withPicks.lineups) === JSON.stringify(clean.lineups),
    "pickedIds changed the result",
  );
}

// --- 3. locks and exclusions on a realistic slate --------------------------
{
  console.log("locks / exclusions / degenerate cases");
  const rand = mulberry(7);
  const all = pool(rand, 149);
  const base: BuildContext = {
    all,
    lockedIds: new Set(),
    excludedIds: new Set(),
    pickedIds: [],
    slots: 6,
    cap: 50000,
  };

  const locked = { ...base, lockedIds: new Set([all[0].id, all[1].id]) };
  const rl = generate(locked, 5, [], 60);
  audit("2 locks", locked, 5, [], 60, rl, null);
  check("2 locks", rl.lineups.length === 5, `added ${rl.lineups.length} (${rl.stop})`);

  // A full roster of locks: exactly one lineup exists, so press two must add
  // nothing and say so rather than looping.
  const allLocked = { ...base, lockedIds: new Set(all.slice(0, 6).map((p) => p.id)) };
  const one = generate(allLocked, 5, [], 100);
  check("6 locks", one.lineups.length === 1, `added ${one.lineups.length}`);
  check("6 locks", one.stop === "exhausted", `stop=${one.stop}`);
  const again = generate(allLocked, 5, one.lineups, 100);
  check("6 locks, press 2", again.lineups.length === 0, `added ${again.lineups.length}`);
  check("6 locks, press 2", again.stop === "exhausted", `stop=${again.stop}`);

  // Cap too low for any roster.
  const broke = generate({ ...base, cap: 1000 }, 5, [], 60);
  check("cap 1000", broke.lineups.length === 0 && broke.stop === "infeasible", broke.stop);

  // Everyone but a bare roster excluded, then that roster saved: the only way
  // left to add a lineup is blocked by exposure, not by the roster space.
  const six = all.slice(0, 6).map((p) => p.id);
  const onlySix = {
    ...base,
    excludedIds: new Set(all.slice(6).map((p) => p.id)),
  };
  const first = generate(onlySix, 1, [], 100);
  if (first.lineups.length === 1) {
    const capped = generate(onlySix, 1, first.lineups, 50);
    check("exposure stop", capped.lineups.length === 0, `added ${capped.lineups.length}`);
    check(
      "exposure stop",
      capped.stop === "exhausted" || capped.stop === "exposure",
      `stop=${capped.stop}`,
    );
  }
  void six;
  void solve;
}

console.log(failures === 0 ? "\nOK — all generate() checks passed" : `\n${failures} FAILURES`);
process.exit(failures === 0 ? 0 : 1);
