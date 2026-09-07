import { useCallback, useEffect, useRef, useState } from "react";
import type { SlateMeta } from "./types";
import { servedOverHttp } from "./loadSlate";

/**
 * Build state: locks, excludes, the current picks, and saved lineups.
 *
 * Stored in two places, deliberately:
 *
 *  1. localStorage — instant, always available, survives a refresh, works from
 *     file://. This is the working copy.
 *  2. current.json in OneDrive — written by the notebook's local server, and
 *     the thing that carries lineups between computers. OneDrive syncs it in
 *     the background; open the dashboard on the other machine and it is there.
 *
 * Note that BOTH the read and the write go through the local server, because
 * the file lives outside the served root. The page only ever knows the
 * relative endpoint below — the server owns the path, which is what lets one
 * identical build work on two machines with different OneDrive roots.
 *
 * Both are keyed on the WEEK, not accumulated. There is exactly one lineup
 * file; it names the tournament it belongs to, so last week's file is ignored
 * rather than deleted, and gets overwritten the moment this week's lineups
 * start being built.
 *
 * Saving is automatic. There is no Save button for this — the button in the
 * rail saves a LINEUP into the saved list, which is a different thing.
 */

/**
 * A saved lineup: its six players, and the name you gave it.
 *
 * There is deliberately no id. The number on the card ("L3") is the lineup's
 * POSITION in this list, computed at render time, so 1..n with no gaps holds
 * by construction — there is no stored value that can drift out of step with
 * the list, and no renumbering step that a code path could forget to call.
 * That is the whole reason the field is gone: a max+1 counter left gaps on
 * delete, and a stored-but-renumbered id just moved the bug somewhere quieter.
 *
 * Position is also the delete handle, which is safe because the list is only
 * ever read and mutated inside one render pass.
 *
 * `name` IS AN ANNOTATION, NOT AN IDENTITY, and that is why adding it does not
 * reintroduce the id. Six golfers joined by a dot is a list you have to READ to
 * tell two builds apart, and by the fifth saved lineup you are comparing rosters
 * character by character. "CHALK", "SCHEFFLER FADE", "TWO LONGSHOTS" is what you
 * were actually thinking when you saved it, and it is the thing the card cannot
 * derive. Absent or blank means unnamed, and an unnamed card shows its position
 * exactly as it always did — so nothing is required of a build you did not want
 * to label.
 *
 * IT DOES NOT PARTICIPATE IN IDENTITY ANYWHERE. Duplicate detection on Save,
 * the exposure counts and Gen's "differs from everything already saved" all key
 * on the SORTED ids and never on the name, so naming two lineups the same thing
 * is allowed and means nothing — which is the correct behaviour for a note to
 * yourself.
 */
export interface SavedLineup {
  ids: string[];
  /** What you called it. Absent or blank means unnamed — see above. */
  name?: string;
}

/** How long a name may be. Not a storage limit — it is the width of the box on
 *  a 278px rail, past which a name is being typed into a slot it cannot be read
 *  back from. */
export const NAME_MAX = 40;

/**
 * RENAME, AND MOVE — the two edits that act on a saved lineup rather than on
 * the build.
 *
 * Pure functions over BuildState, beside `decideSync` and for the same reason:
 * they are the operations most easily got subtly wrong (an off-by-one in the
 * move, a name that grows without bound through the JSON round trip), and a
 * pure function is one that can be proven rather than clicked at. Both return
 * the SAME OBJECT when nothing changes, which is what stops a no-op press from
 * bumping `saved_at` and pushing a pointless write to OneDrive.
 */
export function renameSaved(s: BuildState, index: number, name: string): BuildState {
  if (index < 0 || index >= s.saved.length) return s;
  // CAPPED HERE, NOT WHERE IT IS TYPED. This is where a value stops being
  // whatever the DOM handed us and becomes state — the input's own maxLength is
  // a courtesy to the typist, not the rule.
  const next = name.slice(0, NAME_MAX);
  if ((s.saved[index].name ?? "") === next) return s;
  return { ...s, saved: s.saved.map((l, i) => (i === index ? { ...l, name: next } : l)) };
}

/**
 * Move the card at `from` to `to`, clamping rather than throwing.
 *
 * A NO-OP OFF EITHER END is the contract the UI relies on: the first card keeps
 * its ▲ and the last keeps its ▼, greyed but present (a control that vanished
 * on one card would slide the other two across on exactly that card), and a
 * keypress that gets through costs nothing.
 */
export function moveSaved(s: BuildState, from: number, to: number): BuildState {
  const n = s.saved.length;
  if (from < 0 || from >= n || to < 0 || to >= n || from === to) return s;
  const next = [...s.saved];
  const [moved] = next.splice(from, 1);
  next.splice(to, 0, moved);
  return { ...s, saved: next };
}

export interface BuildState {
  locks: Record<string, true>;
  excludes: Record<string, true>;
  picks: string[];
  saved: SavedLineup[];
  /** ISO timestamp of the last mutation. Decides which copy wins when the
   *  browser and the repo file disagree — see adoptIfNewer below. */
  saved_at?: string;
}

/** What actually goes in the file: the state plus the week it belongs to. */
interface LineupFile extends BuildState {
  tournament: string;
  ending_date: string;
  field_size: number;
}

const EMPTY: BuildState = { locks: {}, excludes: {}, picks: [], saved: [] };

/**
 * Keep only the players and the name, dropping anything else the entry carries.
 *
 * Files written before ids were removed have an `id` on every lineup. Left in
 * place it would round-trip forever through the JSON and reappear on the other
 * machine, so it is stripped on the way in. Entries without a usable `ids`
 * array are dropped rather than rendered as an empty card.
 *
 * THE NAME IS TRIMMED AND CAPPED HERE, at the boundary, and the key is OMITTED
 * rather than written as "" when it is empty — so a file from before names
 * existed and a file whose names were all cleared are the same file, and an
 * empty string cannot travel between machines pretending to be a name. A name
 * that is only whitespace is no name.
 */
function readSaved(raw: unknown): SavedLineup[] {
  if (!Array.isArray(raw)) return [];
  return raw
    .filter((l): l is { ids: string[]; name?: unknown } => !!l && Array.isArray(l.ids))
    .map((l) => {
      const name = typeof l.name === "string" ? l.name.trim().slice(0, NAME_MAX) : "";
      const kept: SavedLineup = { ids: l.ids };
      if (name) kept.name = name;
      return kept;
    });
}

/**
 * What to do on load when the browser's copy and the synced file disagree.
 *
 * Pure and exported so the rule can be proven rather than eyeballed — it is
 * the one piece of this file that can destroy work. The invariant it must hold
 * for every possible pair of inputs: **a side is discarded only in favour of
 * one demonstrably newer**, where a missing timestamp counts as older than any
 * present one, and two indistinguishable sides leave both alone. See
 * test/sync-check.ts.
 *
 *  - "adopt" — take the file. Either it is strictly newer (the other computer
 *    did the work) or there is nothing local to lose.
 *  - "push"  — write the local copy out. It is newer than what is on disk, so
 *    last session's writes did not reach the file: the notebook tab was closed,
 *    which kills the kernel and with it the server that owns the file.
 *  - "none"  — the two already agree, or there is nothing to save. Notably NOT
 *    a write: merely opening the dashboard must not touch the file, or OneDrive
 *    re-uploads it on every load.
 *
 * A file belonging to a different week is not a copy of this state at all —
 * it is last week's, due to be overwritten by this week's first save — so it
 * is treated exactly as if no file existed.
 */
export function decideSync(
  local: BuildState,
  file: Partial<LineupFile> | null,
  meta: { tournament: string; ending_date: string },
): "adopt" | "push" | "none" {
  const sameWeek =
    !!file && file.tournament === meta.tournament && file.ending_date === meta.ending_date;

  const fileTime = sameWeek ? Date.parse(file!.saved_at ?? "") : NaN;
  const localTime = Date.parse(local.saved_at ?? "");
  const haveFileTime = Number.isFinite(fileTime);
  const haveLocalTime = Number.isFinite(localTime);

  if (sameWeek) {
    // An undated file loses to a dated local copy and beats an undated one:
    // without a timestamp the only evidence available is that the file was
    // written by SOMETHING, which is more than an unedited browser has.
    if (haveFileTime && (!haveLocalTime || fileTime > localTime)) return "adopt";
    if (isEmpty(local)) return "adopt";
    if (haveFileTime && haveLocalTime && fileTime === localTime) return "none";
    if (!haveFileTime && !haveLocalTime) return "none";
  }
  // No usable file. Only worth writing if there is something to write.
  return isEmpty(local) ? "none" : "push";
}

/** Same endpoint both ways: GET reads the synced file, POST writes it. */
const LINEUP_ENDPOINT = "/api/lineups";
/** Autosave debounce. Long enough that dragging through a lineup does not
 *  produce a write per click, short enough to survive closing the tab. */
const SAVE_DEBOUNCE_MS = 900;

export type SyncStatus = "local" | "idle" | "saving" | "saved" | "error";

/**
 * Storage key.
 *
 * Tournament + date only. The design handoff also included field size, but that
 * makes the key fragile for no benefit: if DraftKings adds or drops a player
 * mid-week and the notebook is re-run, the field size changes and every lock,
 * exclude and saved lineup silently vanishes. Tournament + date already
 * guarantees a new week gets a new key, which was the actual requirement.
 */
export function storageKey(meta: SlateMeta): string {
  return `pgaslate:v2:${meta.tournament}:${meta.ending_date}`;
}

function read(key: string): BuildState {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return EMPTY;
    const parsed = JSON.parse(raw) as Partial<BuildState>;
    return {
      locks: parsed.locks ?? {},
      excludes: parsed.excludes ?? {},
      picks: Array.isArray(parsed.picks) ? parsed.picks : [],
      saved: readSaved(parsed.saved),
      saved_at: parsed.saved_at,
    };
  } catch {
    // A corrupt entry should not take the app down — this state is disposable.
    return EMPTY;
  }
}

function isEmpty(s: BuildState): boolean {
  return (
    s.picks.length === 0 &&
    s.saved.length === 0 &&
    Object.keys(s.locks).length === 0 &&
    Object.keys(s.excludes).length === 0
  );
}

export function useBuildState(meta: SlateMeta) {
  const key = storageKey(meta);
  const [state, setState] = useState<BuildState>(() => read(key));
  const [status, setStatus] = useState<SyncStatus>(servedOverHttp ? "idle" : "local");

  // Skips the autosave that would otherwise fire immediately on mount, and
  // again right after adopting the repo file — neither is a real edit.
  const skipNextSave = useRef(true);

  // Has the user touched anything since this week's state was loaded? The
  // reconcile below is asynchronous, so an edit can land while its fetch is in
  // flight; from that moment neither branch may run — adopting would discard
  // the edit, and pushing would write a state already superseded by the
  // debounced save, out of order.
  const edited = useRef(false);

  useEffect(() => {
    setState(read(key));
    skipNextSave.current = true;
    edited.current = false;
  }, [key]);

  /** POST a state to the file, reporting the outcome on the badge. */
  const post = useCallback(
    async (s: BuildState) => {
      setStatus("saving");
      const payload: LineupFile = {
        ...s,
        tournament: meta.tournament,
        ending_date: meta.ending_date,
        field_size: meta.field_size,
      };
      try {
        const res = await fetch(LINEUP_ENDPOINT, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        if (!res.ok) throw new Error(String(res.status));
        setStatus("saved");
      } catch {
        // Most likely no server behind the endpoint. localStorage still holds
        // everything, and the badge turns amber to say so.
        setStatus("error");
      }
    },
    [meta.tournament, meta.ending_date, meta.field_size],
  );

  // --- reconcile with the synced file on load ------------------------------
  //
  // Runs in both directions, because either side can be the stale one — which
  // way round is decided by decideSync above.
  //
  // The "push" direction is the one that makes a lost session recoverable.
  // Without it, work stranded in a browser stayed stranded until the next
  // manual edit happened to flush it, which is what made syncing look
  // intermittent: nudge something and last session appeared in OneDrive,
  // look-and-close and it never did. Worse, edit the other machine in the
  // meantime and the stranded copy loses the timestamp comparison and is gone.
  //
  // Pushed verbatim, saved_at included: bumping it would forge a newer edit
  // and let a recovery overwrite genuinely newer work on the other machine.
  useEffect(() => {
    if (!servedOverHttp) return;
    let cancelled = false;

    (async () => {
      const local = read(key);
      let file: Partial<LineupFile> | null = null;
      try {
        const res = await fetch(`${LINEUP_ENDPOINT}?t=${Date.now()}`, { cache: "no-store" });
        // 404 is the normal "no lineups saved for this week yet" answer, and
        // leaves file null — which is precisely a case for pushing.
        if (res.ok) file = (await res.json()) as Partial<LineupFile>;
      } catch {
        // No server listening. Falls through with file null: the push below
        // will fail too, and that failure is the point — it lights the amber
        // badge now rather than after an hour of unsaved lineup building.
      }
      if (cancelled || edited.current) return;

      // "adopt" is only ever returned for a same-week file, so it is non-null.
      const decision = decideSync(local, file, {
        tournament: meta.tournament,
        ending_date: meta.ending_date,
      });
      if (decision === "adopt") {
        skipNextSave.current = true;
        setState({
          locks: file!.locks ?? {},
          excludes: file!.excludes ?? {},
          picks: Array.isArray(file!.picks) ? file!.picks : [],
          saved: readSaved(file!.saved),
          saved_at: file!.saved_at,
        });
      } else if (decision === "push") {
        await post(local);
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [key, meta.tournament, meta.ending_date, post]);

  // --- localStorage: immediate, every change ------------------------------
  useEffect(() => {
    try {
      localStorage.setItem(key, JSON.stringify(state));
    } catch {
      // Quota or private-mode failure. Losing persistence is survivable;
      // crashing the lineup builder is not.
    }
  }, [key, state]);

  // --- synced file: debounced ----------------------------------------------
  useEffect(() => {
    if (!servedOverHttp) return;
    if (skipNextSave.current) {
      skipNextSave.current = false;
      return;
    }

    setStatus("saving");
    const id = setTimeout(() => void post(state), SAVE_DEBOUNCE_MS);
    return () => clearTimeout(id);
  }, [state, post]);

  const update = useCallback((fn: (s: BuildState) => BuildState) => {
    edited.current = true;
    setState((s) => ({ ...fn(s), saved_at: new Date().toISOString() }));
  }, []);

  return [state, update, { status }] as const;
}
