import { decideSync, moveSaved, renameSaved, NAME_MAX } from "../src/persist";
import type { BuildState, SavedLineup } from "../src/persist";

/**
 * The load-time reconcile between the browser's copy and current.json.
 *
 * This is the only decision in the app that can destroy work, and both of its
 * failure modes are silent: adopt too eagerly and a session's lineups vanish
 * with no error, push too eagerly and the other computer's newer work is
 * overwritten. Neither shows up as a crash, a red badge, or anything the user
 * would notice until the lineups are gone. So the rule is proven here rather
 * than read and believed.
 *
 * INVARIANT — for every possible pair of inputs:
 *
 *   A side is discarded only in favour of one demonstrably newer, where a
 *   missing timestamp counts as older than any present one, and two sides
 *   that cannot be told apart leave both alone.
 *
 * Stated operationally, that is three properties, all checked exhaustively
 * over generated pairs at the bottom of this file:
 *
 *   1. Never adopt a file that is older than a non-empty local copy.
 *      (This is the bug that made syncing look intermittent: work stranded by
 *      a dead server got overwritten by the older file on the next load.)
 *   2. Never push a local copy that is older than the file.
 *   3. Never write when the two already agree — opening the dashboard must
 *      not touch the file, or OneDrive re-uploads it on every load.
 */

const WEEK = { tournament: "FedEx St. Jude Championship", ending_date: "2026-08-16" };
const T1 = "2026-08-14T15:00:00.000Z";   // earlier
const T2 = "2026-08-14T16:00:00.000Z";   // later

let failures = 0;

function check(label: string, cond: boolean, detail?: string) {
  if (!cond) {
    failures++;
    console.log(`  FAIL  ${label}${detail ? ` — ${detail}` : ""}`);
  }
}

/** A non-empty build state: one lock, one exclude, a lineup and a saved one. */
function work(saved_at?: string): BuildState {
  return {
    locks: { "Scottie Scheffler": true },
    excludes: { "Justin Rose": true },
    picks: ["a", "b", "c", "d", "e", "f"],
    saved: [{ ids: ["a", "b", "c", "d", "e", "f"] }],
    saved_at,
  };
}

function empty(saved_at?: string): BuildState {
  return { locks: {}, excludes: {}, picks: [], saved: [], saved_at };
}

function fileOf(s: BuildState, week = WEEK) {
  return { ...s, ...week, field_size: 69 };
}

// --- the scenarios that actually happen ------------------------------------

console.log("the real-world sequences");

check(
  "notebook tab closed mid-session: local is newer than the file -> push",
  decideSync(work(T2), fileOf(work(T1)), WEEK) === "push",
);

check(
  "server never came up at all: no file -> push the stranded work",
  decideSync(work(T2), null, WEEK) === "push",
);

check(
  "other computer did the work since: file is newer -> adopt",
  decideSync(work(T1), fileOf(work(T2)), WEEK) === "adopt",
);

check(
  "fresh browser, lineups already in OneDrive -> adopt",
  decideSync(empty(), fileOf(work(T1)), WEEK) === "adopt",
);

check(
  "just opening the dashboard, both sides identical -> no write",
  decideSync(work(T1), fileOf(work(T1)), WEEK) === "none",
);

check(
  "brand new week, nothing anywhere -> no write",
  decideSync(empty(), null, WEEK) === "none",
);

check(
  "last week's file still on disk, this week's work local -> push over it",
  decideSync(work(T1), fileOf(work(T2), { tournament: "Wyndham Championship", ending_date: "2026-08-09" }), WEEK) ===
    "push",
);

check(
  "last week's file, nothing built yet this week -> leave it alone",
  decideSync(empty(), fileOf(work(T2), { tournament: "Wyndham Championship", ending_date: "2026-08-09" }), WEEK) ===
    "none",
);

check(
  "same tournament, different date (a re-run week) is not the same week",
  decideSync(work(T1), fileOf(work(T2), { ...WEEK, ending_date: "2026-08-09" }), WEEK) === "push",
);

// --- untimestamped copies (files written before saved_at existed) ----------

console.log("copies with no timestamp");

check(
  "dated file beats an undated local copy -> adopt",
  decideSync(work(undefined), fileOf(work(T1)), WEEK) === "adopt",
);

check(
  "dated local beats an undated file -> push",
  decideSync(work(T1), fileOf(work(undefined)), WEEK) === "push",
);

check(
  "neither side dated: indistinguishable, so touch nothing",
  decideSync(work(undefined), fileOf(work(undefined)), WEEK) === "none",
);

check(
  "an unparseable timestamp is treated as absent, not as year zero",
  decideSync(work("not a date"), fileOf(work(T1)), WEEK) === "adopt",
);

// --- exhaustive: the three properties over every generated pair ------------

console.log("every combination of (emptiness x timestamp x week)");

const times = [undefined, "not a date", T1, T2];
const bodies: Array<[string, (t?: string) => BuildState]> = [
  ["empty", empty],
  ["work", work],
];
const weeks: Array<[string, typeof WEEK | null]> = [
  ["same week", WEEK],
  ["other week", { tournament: "Wyndham Championship", ending_date: "2026-08-09" }],
  ["no file", null],
];

/** Milliseconds, or null when the stamp is absent/unparseable. */
function stamp(s?: string): number | null {
  const n = Date.parse(s ?? "");
  return Number.isFinite(n) ? n : null;
}

let combos = 0;
for (const [lname, lbody] of bodies) {
  for (const lt of times) {
    for (const [fname, fbody] of bodies) {
      for (const ft of times) {
        for (const [wname, week] of weeks) {
          const local = lbody(lt);
          const file = week ? fileOf(fbody(ft), week) : null;
          const d = decideSync(local, file, WEEK);
          combos++;

          const sameWeek =
            !!file && file.tournament === WEEK.tournament && file.ending_date === WEEK.ending_date;
          const lTime = stamp(lt);
          const fTime = sameWeek ? stamp(ft) : null;
          const localEmpty = lbody === empty;
          const where = `${lname}/${lt} vs ${fname}/${ft} (${wname}) -> ${d}`;

          // 1. Adopting must not throw away newer local work.
          if (d === "adopt" && !localEmpty) {
            const fileIsNewer = fTime !== null && (lTime === null || fTime > lTime);
            check("adopt only over older local work", fileIsNewer, where);
          }

          // 2. Pushing must not overwrite a newer file.
          if (d === "push") {
            const localIsNewer = lTime !== null && (fTime === null || lTime > fTime);
            check("push only over an older file", localIsNewer || !sameWeek, where);
            check("push only when there is something to save", !localEmpty, where);
          }

          // 3. Doing nothing is only allowed when nothing is at stake: no
          //    local work to lose, or the two sides are indistinguishable.
          if (d === "none") {
            const agreed = sameWeek && lTime !== null && fTime !== null && lTime === fTime;
            const bothUndated = sameWeek && lTime === null && fTime === null;
            check("none only when nothing is stranded", localEmpty || agreed || bothUndated, where);
          }

          // 4. Adopt is only ever returned for a file that exists this week —
          //    the caller dereferences it without checking.
          if (d === "adopt") check("adopt implies a same-week file", sameWeek, where);
        }
      }
    }
  }
}

console.log(`  exercised — ${combos} input pairs`);

/* ==========================================================================
   THE SAVED LIST'S OWN EDITS — rename and reorder.

   They are here rather than in build-check because they live in persist.ts and
   because what has to hold of them is a PERSISTENCE property, not a solver one:
   the six players in a lineup are its identity, and neither edit may touch them.
   A rename that dropped a player, or a move that duplicated one, would be
   written straight to OneDrive and carried to the other machine — silently, the
   same way decideSync above can lose work.

   INVARIANTS, over every list position and every input:

     1. THE MULTISET OF LINEUPS IS PRESERVED. Both edits are permutations or
        annotations; no `ids` array is created, destroyed or altered.
     2. A NO-OP RETURNS THE SAME OBJECT. `saved_at` is stamped by the caller on
        every state it is handed a new object for, so an edit that changes
        nothing must be identity or it pushes a pointless write — and, worse,
        forges a newer timestamp that would beat the other machine's real work.
     3. NAMES ARE CAPPED. NAME_MAX is the rule; the input's own maxLength is a
        courtesy to the typist and is not enforcement.
     4. MOVE IS TOTAL. Every index pair, in range or not, returns a legal state.
   ========================================================================== */

function listOf(n: number): BuildState {
  const saved: SavedLineup[] = [];
  for (let i = 0; i < n; i++) saved.push({ ids: [`p${i}a`, `p${i}b`], name: i % 2 ? `L${i}` : undefined });
  return { locks: {}, excludes: {}, picks: [], saved };
}

/** The lineups as comparable strings, order preserved. */
function shape(s: BuildState): string[] {
  return s.saved.map((l) => l.ids.join(","));
}

for (let n = 0; n <= 5; n++) {
  const base = listOf(n);
  const before = shape(base).slice().sort().join("|");

  // ---- moveSaved, over EVERY index pair including out-of-range ones -------
  for (let from = -2; from <= n + 1; from++) {
    for (let to = -2; to <= n + 1; to++) {
      const out = moveSaved(base, from, to);
      const where = `n=${n} ${from}->${to}`;

      // 1 — same lineups, whatever happened to their order.
      check("move preserves the lineups", shape(out).slice().sort().join("|") === before, where);
      check("move preserves the count", out.saved.length === n, where);

      const legal = from >= 0 && from < n && to >= 0 && to < n && from !== to;
      // 2 — identity on a no-op, so no `saved_at` bump and no write.
      if (!legal) check("an out-of-range move is identity", out === base, where);
      else {
        check("a real move returns a new state", out !== base, where);
        // The moved card lands exactly where it was asked to.
        check("the card lands at `to`", shape(out)[to] === shape(base)[from], where);
      }

      // 4 — nothing else on the state is touched.
      check("move leaves the build alone", out.picks === base.picks && out.locks === base.locks, where);
    }
  }

  // ---- renameSaved -------------------------------------------------------
  for (let i = -1; i <= n; i++) {
    const where = `n=${n} i=${i}`;
    const long = "x".repeat(NAME_MAX + 25);
    const out = renameSaved(base, i, long);

    check("rename preserves the lineups", shape(out).slice().sort().join("|") === before, where);
    check("rename preserves the count", out.saved.length === n, where);

    if (i < 0 || i >= n) {
      check("an out-of-range rename is identity", out === base, where);
      continue;
    }
    // 3 — capped here, not at the keyboard.
    check("the name is capped at NAME_MAX", (out.saved[i].name ?? "").length === NAME_MAX, where);
    // Only the named card changed.
    for (let j = 0; j < n; j++) {
      if (j !== i) check("rename touches one card", out.saved[j] === base.saved[j], `${where} j=${j}`);
    }
    // 2 — writing the name it already has is identity.
    check("re-writing the same name is identity", renameSaved(out, i, out.saved[i].name ?? "") === out, where);
    // Clearing a name is a real edit, and leaves it blank rather than absent
    // — `readSaved` is what drops the empty key on the way back in.
    const cleared = renameSaved(out, i, "");
    check("clearing a name is a real edit", cleared !== out && cleared.saved[i].name === "", where);
  }
}

console.log(`  exercised — the saved list's rename and reorder, n=0..5`);

console.log();
if (failures) {
  console.log(`FAILED — ${failures} check(s).`);
  process.exit(1);
}
console.log("PASS — the reconcile never discards work without a newer replacement,");
console.log("       and the saved list's edits never alter a lineup.");
