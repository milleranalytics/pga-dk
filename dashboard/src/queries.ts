import { useCallback, useEffect, useState } from "react";

/**
 * Saved SQL for the DB Query tab.
 *
 * Deliberately NOT keyed on the week, unlike the lineup state in persist.ts. A
 * query you found useful in July is still useful in October; wiping it with the
 * new slate would be the opposite of the behaviour you want.
 *
 * The built-ins below are only a seed. Once the user has a list, it is theirs —
 * editing or deleting a built-in must stick, so the stored list is authoritative
 * from that point on and the seeds are never re-merged.
 */

const KEY = "pgaslate:queries:v1";

export interface SavedQuery {
  name: string;
  sql: string;
}

export const BUILTIN_QUERIES: SavedQuery[] = [
  {
    name: "Career results",
    sql: `SELECT PLAYER, COUNT(*) AS events, ROUND(AVG(FINAL_POS),1) AS avg_finish,
       MIN(FINAL_POS) AS best,
       ROUND(100.0*AVG(CASE WHEN POS NOT IN ('CUT','W/D') THEN 1 ELSE 0 END),0) AS cut_pct
FROM tournaments
GROUP BY PLAYER
HAVING events >= 20
ORDER BY avg_finish
LIMIT 200;`,
  },
  {
    name: "Course leaderboard",
    sql: `SELECT COURSE, PLAYER, COUNT(*) AS events, ROUND(AVG(FINAL_POS),1) AS avg_finish
FROM tournaments
WHERE COURSE = 'Detroit Golf Club'
GROUP BY COURSE, PLAYER
HAVING events >= 2
ORDER BY avg_finish
LIMIT 200;`,
  },
  {
    name: "Predictions vs results",
    sql: `SELECT p.ENDING_DATE, p.TOURNAMENT, p.PLAYER,
       ROUND(p.P_TOP20,3) AS pred, t.POS, t.FINAL_POS
FROM predictions p
LEFT JOIN tournaments t
  ON t.TOURNAMENT = p.TOURNAMENT
 AND t.ENDING_DATE = p.ENDING_DATE
 AND t.PLAYER = p.PLAYER
ORDER BY p.ENDING_DATE DESC, pred DESC
LIMIT 500;`,
  },
  {
    name: "Season stats leaders",
    sql: `SELECT PLAYER, SGTTG, SGOTT, SGAPR, SGATG, SGP, OWGR_RANK
FROM stats
WHERE SEASON = (SELECT MAX(SEASON) FROM stats) AND SGTTG IS NOT NULL
ORDER BY SGTTG DESC
LIMIT 200;`,
  },
  {
    name: "Odds coverage",
    sql: `SELECT SEASON, COUNT(DISTINCT TOURNAMENT) AS events, COUNT(*) AS odds_rows
FROM odds
GROUP BY SEASON
ORDER BY SEASON DESC;`,
  },
  {
    name: "Name drift check",
    sql: `-- Prediction-log names that match nothing in the results table.
-- The signature of a spelling mismatch; should return zero rows.
SELECT DISTINCT p.PLAYER
FROM predictions p
LEFT JOIN tournaments t ON t.PLAYER = p.PLAYER
WHERE t.PLAYER IS NULL
ORDER BY p.PLAYER;`,
  },
];

function read(): SavedQuery[] {
  try {
    const raw = localStorage.getItem(KEY);
    if (!raw) return BUILTIN_QUERIES;
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return BUILTIN_QUERIES;
    return parsed.filter(
      (q): q is SavedQuery => q && typeof q.name === "string" && typeof q.sql === "string",
    );
  } catch {
    return BUILTIN_QUERIES;
  }
}

export function useSavedQueries() {
  const [queries, setQueries] = useState<SavedQuery[]>(() => read());

  useEffect(() => {
    try {
      localStorage.setItem(KEY, JSON.stringify(queries));
    } catch {
      // Losing saved queries is survivable; crashing the tab is not.
    }
  }, [queries]);

  /** Save by name — same name overwrites, so editing a query and re-saving it
   *  updates in place instead of quietly accumulating duplicates. */
  const save = useCallback((name: string, sql: string) => {
    const trimmed = name.trim();
    if (!trimmed) return;
    setQueries((qs) => {
      const i = qs.findIndex((q) => q.name.toLowerCase() === trimmed.toLowerCase());
      if (i >= 0) {
        const next = [...qs];
        next[i] = { name: trimmed, sql };
        return next;
      }
      return [...qs, { name: trimmed, sql }];
    });
  }, []);

  const remove = useCallback((name: string) => {
    setQueries((qs) => qs.filter((q) => q.name !== name));
  }, []);

  const resetToBuiltins = useCallback(() => setQueries(BUILTIN_QUERIES), []);

  return { queries, save, remove, resetToBuiltins };
}
