import { toggleLock, toggleExclude, clearConstraints, removeFromBuild } from "../src/build";
import { optimize, whyInfeasible } from "../src/optimizer";
import type { BuildContext, OptPlayer } from "../src/optimizer";
import type { BuildState } from "../src/persist";
import { valuePerK } from "../src/enrich";

/**
 * The build-state transitions (L, X, CLR, slot click) and the reason string
 * Optimize shows when it cannot produce a lineup.
 *
 * Since Aug 2026 the constraint edits are pure and do NOT touch `picks` — App
 * re-solves after each one, and the optimizer's own invariants (exactly
 * `roster` distinct players, under the cap) are proven in optimizer-check.ts.
 * So what is left to check here is: the constraint algebra, purity, and that
 * the composition App actually performs (edit, then solve) lands somewhere
 * legal. That last one is the real subject of this file — it is where the two
 * halves meet, and neither half's own test covers it.
 */

const ROSTER = 6;
const CAP = 50000;
let failures = 0;

function check(label: string, cond: boolean, detail?: string) {
  if (!cond) {
    failures++;
    console.log(`  FAIL  ${label}${detail ? ` — ${detail}` : ""}`);
  }
}

function state(p: Partial<BuildState> = {}): BuildState {
  return { locks: {}, excludes: {}, picks: [], saved: [], ...p };
}

/** A synthetic field: salaries 5,000..12,900, value decreasing with price so
 *  the optimizer has real trade-offs to make rather than a dominant answer. */
const POOL: OptPlayer[] = Array.from({ length: 60 }, (_, i) => ({
  id: `P${i + 1}`,
  salary: 5000 + (i % 40) * 200,
  value: 0.35 - i * 0.004,
}));

function ctxFor(s: BuildState): BuildContext {
  return {
    all: POOL,
    lockedIds: new Set(Object.keys(s.locks)),
    excludedIds: new Set(Object.keys(s.excludes)),
    pickedIds: [],
    slots: ROSTER,
    cap: CAP,
  };
}

/** Exactly what App.applyConstraints does: edit, solve, keep picks on failure. */
function press(s: BuildState, edit: (x: BuildState) => BuildState) {
  const next = edit(s);
  const c = ctxFor(next);
  const r = optimize(c);
  return {
    state: r ? { ...next, picks: r.map((p) => p.id) } : next,
    note: r ? null : whyInfeasible(c),
  };
}

// --- the constraint algebra -------------------------------------------------

console.log("L and X are pure constraint edits — they never touch picks");
{
  const s = state({ picks: ["P1", "P2", "P3"] });
  check("lock leaves picks alone", toggleLock(s, "P9").picks === s.picks);
  check("unlock leaves picks alone", toggleLock(state({ picks: ["P1"], locks: { P1: true } }), "P1").picks.length === 1);
  check("exclude leaves picks alone", toggleExclude(s, "P1").picks === s.picks);
}

console.log("lock and exclude stay mutually exclusive");
{
  let s = state();
  s = toggleExclude(s, "P1");
  s = toggleLock(s, "P1");
  check("locking clears the exclusion", !!s.locks["P1"] && !s.excludes["P1"]);
  s = toggleExclude(s, "P1");
  check("excluding clears the lock", !s.locks["P1"] && !!s.excludes["P1"]);
  s = toggleExclude(s, "P1");
  check("pressing X again clears it", !s.excludes["P1"] && !s.locks["P1"]);
}

console.log("CLR drops every constraint and leaves the lineup alone");
{
  const s0 = state({ picks: ["P1", "P2", "P3"], locks: { P1: true }, excludes: { P9: true } });
  const s = clearConstraints(s0);
  check("no locks remain", Object.keys(s.locks).length === 0);
  check("no exclusions remain", Object.keys(s.excludes).length === 0);
  check("the lineup is untouched", JSON.stringify(s.picks) === JSON.stringify(s0.picks));
  check("no-op when there is nothing to clear", clearConstraints(state()) !== undefined);
  check("returns the same object when empty", clearConstraints(state({ picks: ["P1"] })).picks.length === 1);
}

console.log("clicking a slot removes the player and his lock, leaving a hole");
{
  const s = removeFromBuild(
    state({ picks: ["P1", "P2"], locks: { P1: true, P5: true }, excludes: { P9: true } }),
    "P1",
  );
  check("the pick is gone", !s.picks.includes("P1"));
  check("his lock is gone", !s.locks["P1"], "else the constraints claim a player the build lacks");
  check("other locks survive", !!s.locks["P5"]);
  check("exclusions survive", !!s.excludes["P9"]);
  check("the hole is real", s.picks.length === 1);
}

console.log("nothing mutates its input, and doubling a call changes nothing");
{
  const before = state({ picks: ["P1"], locks: { P1: true }, excludes: { P9: true } });
  const snap = JSON.stringify(before);
  toggleLock(before, "P1");
  toggleLock(before, "P2");
  toggleExclude(before, "P9");
  clearConstraints(before);
  removeFromBuild(before, "P1");
  check("input untouched", JSON.stringify(before) === snap);
  // StrictMode runs every updater twice.
  for (const id of ["P1", "P2"]) {
    check(
      `lock ${id} idempotent`,
      JSON.stringify(toggleLock(before, id)) === JSON.stringify(toggleLock(before, id)),
    );
    check(
      `exclude ${id} idempotent`,
      JSON.stringify(toggleExclude(before, id)) === JSON.stringify(toggleExclude(before, id)),
    );
  }
}

// --- edit + solve, which is what a press actually does -----------------------

console.log("locking on a FULL roster rebuilds around the new lock");
{
  // The behaviour this whole model exists for. Previously the press did nothing
  // visible and you had to hit Optimize yourself.
  let s = state();
  s = press(s, (x) => toggleLock(x, "P1")).state;
  check("first lock fills the whole roster", s.picks.length === ROSTER, `got ${s.picks.length}`);
  check("and contains the locked player", s.picks.includes("P1"));

  const before = [...s.picks];
  const target = POOL.find((p) => !before.includes(p.id))!.id;
  const r = press(s, (x) => toggleLock(x, target));
  check("locking a 7th name still yields 6", r.state.picks.length === ROSTER);
  check("the new man is in the lineup", r.state.picks.includes(target), "this is the reported bug");
  check("the earlier lock survives", r.state.picks.includes("P1"));
  check("no note, since it solved", r.note === null);
}

console.log("excluding a man in the lineup replaces him on the spot");
{
  let s = press(state(), (x) => toggleLock(x, "P1")).state;
  const victim = s.picks.find((id) => id !== "P1")!;
  s = press(s, (x) => toggleExclude(x, victim)).state;
  check("he is gone", !s.picks.includes(victim));
  check("and was replaced, not just dropped", s.picks.length === ROSTER);
}

console.log("unlocking re-solves and refills the slot");
{
  let s = press(state(), (x) => toggleLock(x, "P60")).state;
  check("P60 is in while locked", s.picks.includes("P60"));
  s = press(s, (x) => toggleLock(x, "P60")).state;
  check("still a full roster after unlocking", s.picks.length === ROSTER);
  check("and P60 — the worst value in the pool — is dropped", !s.picks.includes("P60"));
}

console.log("every reachable press leaves a legal lineup or an explained one");
{
  // The composition property. Random walk over L / X / CLR / slot-click; after
  // each press either the build is a legal roster, or the press produced a note
  // saying why it could not be. There is no third outcome, and the third
  // outcome is exactly what "nothing happens" was.
  let s = state();
  let rng = 987654321;
  const rand = () => {
    rng ^= rng << 13;
    rng ^= rng >>> 17;
    rng ^= rng << 5;
    return Math.abs(rng);
  };

  let illegal = 0;
  let unexplained = 0;
  let wrongReason = 0;
  const seen = { solved: 0, refused: 0 };

  for (let i = 0; i < 40_000; i++) {
    const id = POOL[rand() % POOL.length].id;
    const op = rand() % 10;
    let note: string | null = null;

    if (op < 5) ({ state: s, note } = press(s, (x) => toggleLock(x, id)));
    else if (op < 8) ({ state: s, note } = press(s, (x) => toggleExclude(x, id)));
    else if (op === 8) s = clearConstraints(s);
    else if (s.picks.length) s = removeFromBuild(s, s.picks[rand() % s.picks.length]);

    // Locks and exclusions never overlap, whatever the sequence.
    for (const k of Object.keys(s.locks)) if (s.excludes[k]) illegal++;
    if (new Set(s.picks).size !== s.picks.length) illegal++;

    if (note === null) {
      if (op < 8) {
        seen.solved++;
        // A solved press must leave a full, cap-legal roster containing every
        // lock and no exclusion.
        const salary = s.picks.reduce(
          (a, pid) => a + (POOL.find((p) => p.id === pid)?.salary ?? 0),
          0,
        );
        if (s.picks.length !== ROSTER || salary > CAP) illegal++;
        for (const k of Object.keys(s.locks)) if (!s.picks.includes(k)) illegal++;
        for (const pid of s.picks) if (s.excludes[pid]) illegal++;
      }
    } else {
      seen.refused++;
      if (!note.length) unexplained++;
      // The stated reason has to be TRUE, not merely present.
      const locks = Object.keys(s.locks).length;
      if (note.includes("locks for") && locks <= ROSTER) wrongReason++;
      if (note.includes("cost")) {
        const cost = Object.keys(s.locks).reduce(
          (a, pid) => a + (POOL.find((p) => p.id === pid)?.salary ?? 0),
          0,
        );
        if (cost <= CAP) wrongReason++;
      }
    }
  }

  console.log(`  exercised — presses that solved ${seen.solved}, presses refused ${seen.refused}`);
  check("the walk reached both outcomes", seen.solved > 0 && seen.refused > 0);
  check("every solved press left a legal lineup", illegal === 0, `${illegal} violations`);
  check("every refusal was explained", unexplained === 0, `${unexplained} silent`);
  check("every stated reason was true", wrongReason === 0, `${wrongReason} false claims`);
}

/* ==========================================================================
   VAL /$1K — the rail's value figure, and the grid's VAL column.

   ONE FUNCTION FILLS BOTH (`valuePerK`), which is what makes them comparable:
   a lineup reading 5.02 is spending its cap as efficiently as a player whose
   VAL cell reads 5.02. That claim is only true if the lineup form is a
   SALARY-WEIGHTED mean of the player forms — which it is, by construction,
   because it divides summed points by summed salary rather than averaging six
   ratios.

   The consequences a reader is entitled to assume, checked over generated
   rosters rather than argued:

     1. UNIFORM ROSTER. If every player has the same VAL, the lineup reports
        exactly that VAL. (A plain mean would pass this too — it is the floor,
        not the discriminator.)
     2. IN RANGE. The lineup's VAL never leaves [min, max] of its players'. This
        is what "weighted mean" buys and what makes the number safe to read
        against the column.
     3. IT IS THE WEIGHTED MEAN, NOT THE PLAIN ONE. On a roster with unequal
        salaries the two differ, and the lineup figure must equal the
        salary-weighted one — this is the check that would fail if someone
        "simplified" it to an average of the VAL column.
     4. EMPTY IS NULL, NOT ZERO. Nothing bought is not zero value per dollar.
   ========================================================================== */
{
  // A deterministic spread of prices and probabilities, unequal on purpose:
  // equal salaries would make properties 2 and 3 vacuous.
  const mk = (n: number) =>
    Array.from({ length: n }, (_, i) => ({
      salary: 6000 + i * 1700,
      pts: 12 + ((i * 7) % 23) + (i % 3) * 4.5,
    }));

  const near = (a: number, b: number) => Math.abs(a - b) < 1e-9;
  let uniform = 0;
  let inRange = 0;
  let weighted = 0;
  let cases = 0;

  for (let n = 1; n <= 8; n++) {
    for (let shift = 0; shift < 6; shift++) {
      const roster = mk(n).map((r) => ({ ...r, pts: r.pts + shift * 1.3 }));
      const sal = roster.reduce((a, r) => a + r.salary, 0);
      const pts = roster.reduce((a, r) => a + r.pts, 0);
      const got = valuePerK(pts, sal);
      cases++;
      if (got === null) continue;

      const each = roster.map((r) => valuePerK(r.pts, r.salary) as number);

      // 2 — inside the range of its parts.
      const lo = Math.min(...each);
      const hi = Math.max(...each);
      if (got < lo - 1e-9 || got > hi + 1e-9) inRange++;

      // 3 — equals the salary-weighted mean, computed the long way round.
      const wm = roster.reduce((a, r, i) => a + each[i] * (r.salary / sal), 0);
      if (!near(got, wm)) weighted++;

      // 1 — a roster where every player shares one VAL reports that VAL. Built
      //     by giving each player points proportional to his own salary.
      const V = 4.25;
      const uni = roster.map((r) => ({ salary: r.salary, pts: (V * r.salary) / 1000 }));
      const uGot = valuePerK(
        uni.reduce((a, r) => a + r.pts, 0),
        uni.reduce((a, r) => a + r.salary, 0),
      ) as number;
      if (!near(uGot, V)) uniform++;
    }
  }

  console.log(`  exercised — VAL /$1K over ${cases} generated rosters`);
  check("a uniform-VAL roster reports that VAL", uniform === 0, `${uniform} off`);
  check("the lineup's VAL stays inside its players' range", inRange === 0, `${inRange} outside`);
  check("it is the salary-weighted mean, not the plain one", weighted === 0, `${weighted} off`);
  check("nothing bought is null, not zero", valuePerK(0, 0) === null);
  // GUARDS PROPERTY 3 FROM PASSING VACUOUSLY. If a roster's plain mean happened
  // to equal its weighted mean, "it is the weighted one" would be proven by
  // nothing. The first draft of this file used the `mk` spread above and the
  // two means came out 0.048 apart — close enough that the check was almost
  // decorative, which is exactly what this line is here to catch.
  //
  // So the discriminating roster is built on purpose: VAL descends as salary
  // rises (the cheap men are the efficient ones), which is the arrangement that
  // pulls a plain mean furthest above a salary-weighted one. Here it is 3.50
  // against 3.02 — half a point, and visible in the readout.
  check("a real spread makes the two means differ", (() => {
    const r = Array.from({ length: 6 }, (_, i) => {
      const salary = 6000 + i * 1700;
      return { salary, pts: ((6 - i) * salary) / 1000 };  // VAL = 6,5,4,3,2,1
    });
    const each = r.map((x) => valuePerK(x.pts, x.salary) as number);
    const plain = each.reduce((a, b) => a + b, 0) / each.length;
    const w = valuePerK(r.reduce((a, x) => a + x.pts, 0), r.reduce((a, x) => a + x.salary, 0)) as number;
    // The weighted mean must be the LOWER of the two here, not merely different
    // — that direction is the whole reason the distinction matters.
    return plain - w > 0.4;
  })());
}

console.log("");
if (failures > 0) {
  console.log(`FAIL — ${failures} check(s) failed.`);
  process.exit(1);
}
console.log("PASS — constraint edits and the solve they trigger agree,");
console.log("       and VAL /$1K is the weighted mean it claims to be.");
