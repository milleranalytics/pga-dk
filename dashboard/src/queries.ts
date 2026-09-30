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
 *
 * The key is pga.db's own: the list under the original key named golf.db's
 * tables, which pga.db does not have.
 */

const KEY = "pgaslate:queries:pga:v1";

export interface SavedQuery {
  name: string;
  sql: string;
}

/** pga.db: the Tour's ids everywhere, names in `players`. v_results (built at
 *  load, db.ts) has the names attached; fold() takes accents off for a search. */
export const BUILTIN_QUERIES: SavedQuery[] = [
  {
    name: "This week's SG ratings",
    sql: `-- The model's strokes-gained ratings for the latest event's field, as of its first day.
SELECT p.name AS player, ROUND(s.SGA_TOTAL,2) AS total, ROUND(s.SGA_T2G,2) AS t2g,
       ROUND(s.SGA_OTT,2) AS ott, ROUND(s.SGA_APP,2) AS app, ROUND(s.SGA_ARG,2) AS arg,
       ROUND(s.SGA_PUTT,2) AS putt, s.SGA_ROUNDS_12M AS rounds_12m
FROM sg_form s
JOIN players p ON p.player_id = s.player_id
WHERE s.tournament_id = (SELECT f.tournament_id FROM sg_form f
                         JOIN events e ON e.tournament_id = f.tournament_id
                         ORDER BY e.start_date DESC LIMIT 1)
ORDER BY s.SGA_TOTAL DESC;`,
  },
  {
    name: "One golfer's ratings over time",
    sql: `SELECT e.start_date, e.name AS event, ROUND(s.SGA_TOTAL,2) AS total,
       ROUND(s.SGA_OTT,2) AS ott, ROUND(s.SGA_APP,2) AS app, ROUND(s.SGA_ARG,2) AS arg,
       ROUND(s.SGA_PUTT,2) AS putt, s.SGA_ROUNDS_12M AS rounds_12m, r.position
FROM sg_form s
JOIN events e ON e.tournament_id = s.tournament_id
JOIN players p ON p.player_id = s.player_id
LEFT JOIN results r ON r.tournament_id = s.tournament_id AND r.player_id = s.player_id
WHERE fold(p.name) LIKE '%scheffler%'
ORDER BY e.start_date DESC;`,
  },
  {
    name: "Round-by-round SG, one event",
    sql: `SELECT p.name AS player, s.round, s.sg_ott, s.sg_app, s.sg_arg, s.sg_putt, s.sg_total
FROM sg_rounds s
JOIN players p ON p.player_id = s.player_id
WHERE s.tournament_id = (SELECT tournament_id FROM events WHERE name LIKE '%TOUR Championship%'
                         ORDER BY end_date DESC LIMIT 1)
ORDER BY s.round, s.sg_total DESC;`,
  },
  {
    name: "Career results",
    sql: `SELECT player, COUNT(*) AS events, ROUND(AVG(finish_rank),1) AS avg_finish,
       MIN(finish_rank) AS best,
       ROUND(100.0*AVG(CASE WHEN position NOT IN ('CUT','W/D','DQ') THEN 1 ELSE 0 END),0) AS cut_pct,
       ROUND(AVG(sg_total),2) AS sg_per_round
FROM v_results
GROUP BY player_id
HAVING events >= 20
ORDER BY sg_per_round DESC
LIMIT 200;`,
  },
  {
    name: "Course leaderboard",
    sql: `SELECT player, COUNT(*) AS events, ROUND(AVG(finish_rank),1) AS avg_finish,
       ROUND(AVG(sg_total),2) AS sg_per_round
FROM v_results
WHERE course LIKE '%Black Desert%'
GROUP BY player_id
HAVING events >= 2
ORDER BY sg_per_round DESC;`,
  },
  {
    name: "Predictions vs results",
    sql: `SELECT e.end_date, e.name AS event, p.name AS player, ROUND(x.p_top20,3) AS pred,
       r.position, r.finish_rank
FROM predictions x
JOIN events e ON e.tournament_id = x.tournament_id
JOIN players p ON p.player_id = x.player_id
LEFT JOIN results r ON r.tournament_id = x.tournament_id AND r.player_id = x.player_id
ORDER BY e.end_date DESC, pred DESC
LIMIT 500;`,
  },
  {
    name: "Season stats leaders",
    sql: `-- The Tour's own season stats (not the model's ratings), latest season.
SELECT p.name AS player,
       MAX(CASE WHEN s.stat='SGTTG' THEN s.value END) AS sgttg,
       MAX(CASE WHEN s.stat='SGOTT' THEN s.value END) AS sgott,
       MAX(CASE WHEN s.stat='SGAPR' THEN s.value END) AS sgapr,
       MAX(CASE WHEN s.stat='SGATG' THEN s.value END) AS sgatg,
       MAX(CASE WHEN s.stat='SGP' THEN s.value END) AS sgp
FROM season_stats s
JOIN players p ON p.player_id = s.player_id
WHERE s.season = (SELECT MAX(season) FROM season_stats)
GROUP BY s.player_id
HAVING sgttg IS NOT NULL
ORDER BY sgttg DESC;`,
  },
  {
    name: "Odds coverage",
    sql: `SELECT e.season, COUNT(DISTINCT o.tournament_id) AS events, COUNT(*) AS odds_rows
FROM odds o
JOIN events e ON e.tournament_id = o.tournament_id
GROUP BY e.season
ORDER BY e.season DESC;`,
  },
  {
    name: "Names that needed help",
    sql: `-- How each outside name (odds board, DraftKings, old predictions) was matched to a
-- Tour player id. 'field' = found in that week's field by spelling; the rest are worth a look.
SELECT source, how, COUNT(*) AS names, SUM(rows) AS rows
FROM name_resolution
GROUP BY source, how
ORDER BY source, names DESC;`,
  },
];

function read(): SavedQuery[] {
  const builtins = BUILTIN_QUERIES;
  try {
    const raw = localStorage.getItem(KEY);
    if (!raw) return builtins;
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return builtins;
    return parsed.filter(
      (q): q is SavedQuery => q && typeof q.name === "string" && typeof q.sql === "string",
    );
  } catch {
    return builtins;
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
