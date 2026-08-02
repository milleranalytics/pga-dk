import { useCallback, useEffect, useState } from "react";
import type { SlateMeta } from "./types";

/**
 * localStorage-backed build state.
 *
 * The key is derived from tournament + date + field size, which is the whole
 * design: a new week's export produces a new key, so last week's locks,
 * excludes and saved lineups disappear on their own. No cleanup step, no stale
 * lineups quietly carried into a different field.
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
}

const EMPTY: BuildState = { locks: {}, excludes: {}, picks: [], saved: [] };

export function storageKey(meta: SlateMeta): string {
  return `pgaslate:v1:${meta.tournament}:${meta.ending_date}:${meta.field_size}`;
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
    };
  } catch {
    // A corrupt or unparseable entry should not take the app down — the whole
    // point of this state is that it is disposable.
    return EMPTY;
  }
}

export function useBuildState(meta: SlateMeta) {
  const key = storageKey(meta);
  const [state, setState] = useState<BuildState>(() => read(key));

  // Re-read when the key changes (a new week's slate loaded).
  useEffect(() => {
    setState(read(key));
  }, [key]);

  useEffect(() => {
    try {
      localStorage.setItem(key, JSON.stringify(state));
    } catch {
      // Quota or private-mode failure. Losing persistence is survivable;
      // crashing the lineup builder is not.
    }
  }, [key, state]);

  const update = useCallback(
    (fn: (s: BuildState) => BuildState) => setState((s) => fn(s)),
    [],
  );

  return [state, update] as const;
}
