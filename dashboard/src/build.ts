import type { BuildState } from "./persist";

/**
 * The build-state transitions, as pure functions.
 *
 * THE MODEL, and it changed in Aug 2026 — read this before editing:
 *
 *   `locks` and `excludes` are the CONSTRAINTS. `picks` is the SOLVER'S OUTPUT.
 *
 * Every L and X press edits the constraints, and App re-solves immediately, so
 * the lineup on screen is always the best roster satisfying what you have
 * asked for. Locking is therefore "put this man in my lineup and rebuild around
 * him", in one press, which is what pressing L and then Optimize always meant.
 *
 * That is why NONE of the constraint edits below touch `picks`. They used to —
 * lock appended the player, unlock removed him, and lock on a full roster had
 * to decide whether to evict someone. All of that was doing the optimizer's job
 * by hand and doing it worse: appending a seventh player would have produced a
 * roster DraftKings does not accept, and the rail has only `roster` slots to
 * draw him in. The solver answers the same question correctly and in about a
 * millisecond.
 *
 * The one exception is `removeFromBuild`, the rail's slot click, which edits the
 * build directly and deliberately leaves a hole. See its comment.
 *
 * INVARIANTS. Every function returns a state satisfying these, given an input
 * that satisfies them:
 *
 *  1. `locks` and `excludes` are disjoint — "force into every solve" and
 *     "remove from every solve" cannot both be true of one player.
 *  2. `picks` holds no duplicates.
 *  3. The input state is never mutated, and applying a transition twice gives
 *     the same result as applying it once (StrictMode double-invokes updaters).
 *
 * Roster size and the salary cap are NOT invariants of this file. They are
 * properties of what the optimizer returns, proven in test/optimizer-check.ts.
 * A failed solve leaves `picks` untouched, which is the only way this state can
 * hold a lineup that does not match the constraints — App reports that case in
 * the rail rather than letting it pass silently.
 */

/**
 * L — lock and unlock. A pure constraint edit; the caller re-solves.
 *
 * Locking clears any exclusion on the same player: "force in" and "keep out"
 * cannot both be true, and setting one is an unambiguous statement about which
 * you meant.
 */
export function toggleLock(s: BuildState, id: string): BuildState {
  const locks = { ...s.locks };
  const excludes = { ...s.excludes };
  if (locks[id]) {
    delete locks[id];
  } else {
    locks[id] = true;
    delete excludes[id];
  }
  return { ...s, locks, excludes };
}

/**
 * X — exclude and un-exclude. A pure constraint edit; the caller re-solves.
 *
 * Excluding a player who is in the build no longer needs to remove him: he is
 * out of the solver's pool, so the next solve — which happens on this very
 * press — cannot return him. The old "a player can be both in the build and
 * excluded" carve-out is gone with it, because there is no longer a gap between
 * setting a constraint and the build obeying it.
 */
export function toggleExclude(s: BuildState, id: string): BuildState {
  const locks = { ...s.locks };
  const excludes = { ...s.excludes };
  if (excludes[id]) {
    delete excludes[id];
  } else {
    excludes[id] = true;
    delete locks[id];
  }
  return { ...s, locks, excludes };
}

/**
 * CLR — drop every lock and exclusion at once.
 *
 * It leaves `picks` alone and does NOT re-solve, which makes it the one
 * constraint edit that changes nothing on the rail. That is deliberate and it
 * is what "clear my constraints" should mean: the lineup you are looking at is
 * still a valid, cap-legal roster, and it is now simply unconstrained. Clearing
 * used to empty the slots the locks were holding, which under the old model was
 * consistent but made a button named CLR capable of wiping a finished lineup.
 * Re-solving instead would be the opposite overreach — silently replacing your
 * lineup with the unconstrained optimum, which is what Optimize is for.
 */
export function clearConstraints(s: BuildState): BuildState {
  if (Object.keys(s.locks).length === 0 && Object.keys(s.excludes).length === 0) return s;
  return { ...s, locks: {}, excludes: {} };
}

/**
 * Clicking a filled lineup slot — the only edit that touches the build directly.
 *
 * Drops the player AND any lock on him, and does NOT re-solve, so the slot
 * visibly empties and stays empty. That is the whole point of it: it is the one
 * way to say "show me this lineup one player short" and have it stick. Routing
 * it through `toggleLock` would re-solve, and the optimizer would hand the slot
 * straight back to the same player whenever he was the best available — a
 * remove button that visibly does nothing, which is exactly the complaint that
 * produced the auto-solve model in the first place.
 *
 * The lock has to go with him or the constraints would claim a player the build
 * does not contain, and the next solve would undo the removal anyway.
 */
export function removeFromBuild(s: BuildState, id: string): BuildState {
  const locks = { ...s.locks };
  delete locks[id];
  return { ...s, locks, picks: s.picks.filter((x) => x !== id) };
}
