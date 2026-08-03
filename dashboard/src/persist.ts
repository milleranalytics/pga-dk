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
 *  2. data/lineups/current.json in the repo — written by the notebook's local
 *     server, and the thing that carries lineups between computers. Commit it
 *     at one machine, pull at the other, and the lineups follow.
 *
 * Both are keyed on the WEEK, not accumulated. There is exactly one lineup
 * file; it names the tournament it belongs to, so last week's file is ignored
 * rather than deleted, and gets overwritten the moment this week's lineups
 * start being built.
 *
 * Saving is automatic. There is no Save button for this — the button in the
 * rail saves a LINEUP into the saved list, which is a different thing.
 */

export interface SavedLineup {
  id: number;
  ids: string[];
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

const LINEUP_URL = "/data/lineups/current.json";
const SAVE_ENDPOINT = "/api/lineups";
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
      saved: Array.isArray(parsed.saved) ? parsed.saved : [],
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
  const [lastSaved, setLastSaved] = useState<string | null>(null);

  // Skips the autosave that would otherwise fire immediately on mount, and
  // again right after adopting the repo file — neither is a real edit.
  const skipNextSave = useRef(true);

  useEffect(() => {
    setState(read(key));
    skipNextSave.current = true;
  }, [key]);

  // --- adopt the repo's copy when it is newer -----------------------------
  useEffect(() => {
    if (!servedOverHttp) return;
    let cancelled = false;

    (async () => {
      try {
        const res = await fetch(`${LINEUP_URL}?t=${Date.now()}`, { cache: "no-store" });
        if (!res.ok || cancelled) return;
        const file = (await res.json()) as Partial<LineupFile>;

        // Belongs to a different week — ignore it entirely. It will be
        // overwritten as soon as anything is built this week.
        if (file.tournament !== meta.tournament || file.ending_date !== meta.ending_date) {
          return;
        }

        const local = read(key);
        const fileTime = Date.parse(file.saved_at ?? "");
        const localTime = Date.parse(local.saved_at ?? "");
        // Adopt when the file is strictly newer, or when there is nothing
        // local to lose. Never clobber newer local work with an older pull.
        const adopt =
          (Number.isFinite(fileTime) && (!Number.isFinite(localTime) || fileTime > localTime)) ||
          isEmpty(local);
        if (!adopt || cancelled) return;

        skipNextSave.current = true;
        setState({
          locks: file.locks ?? {},
          excludes: file.excludes ?? {},
          picks: Array.isArray(file.picks) ? file.picks : [],
          saved: Array.isArray(file.saved) ? file.saved : [],
          saved_at: file.saved_at,
        });
      } catch {
        // No file yet, or not served — the local copy stands.
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [key, meta.tournament, meta.ending_date]);

  // --- localStorage: immediate, every change ------------------------------
  useEffect(() => {
    try {
      localStorage.setItem(key, JSON.stringify(state));
    } catch {
      // Quota or private-mode failure. Losing persistence is survivable;
      // crashing the lineup builder is not.
    }
  }, [key, state]);

  // --- repo file: debounced ------------------------------------------------
  useEffect(() => {
    if (!servedOverHttp) return;
    if (skipNextSave.current) {
      skipNextSave.current = false;
      return;
    }

    setStatus("saving");
    const payload: LineupFile = {
      ...state,
      tournament: meta.tournament,
      ending_date: meta.ending_date,
      field_size: meta.field_size,
    };

    const id = setTimeout(async () => {
      try {
        const res = await fetch(SAVE_ENDPOINT, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        if (!res.ok) throw new Error(String(res.status));
        setStatus("saved");
        setLastSaved(new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }));
      } catch {
        // Most likely the page is open against a plain static server with no
        // POST handler. localStorage still holds everything.
        setStatus("error");
      }
    }, SAVE_DEBOUNCE_MS);

    return () => clearTimeout(id);
  }, [state, meta.tournament, meta.ending_date, meta.field_size]);

  const update = useCallback(
    (fn: (s: BuildState) => BuildState) =>
      setState((s) => ({ ...fn(s), saved_at: new Date().toISOString() })),
    [],
  );

  return [state, update, { status, lastSaved }] as const;
}
