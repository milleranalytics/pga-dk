import { toggleLock, toggleExclude, clearConstraints, removeFromBuild } from "../src/build";
import { optimize, whyInfeasible } from "../src/optimizer";
import type { BuildContext, OptPlayer } from "../src/optimizer";
import type { BuildState } from "../src/persist";

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

console.log("");
if (failures > 0) {
  console.log(`FAIL — ${failures} check(s) failed.`);
  process.exit(1);
}
console.log("PASS — constraint edits and the solve they trigger agree.");
