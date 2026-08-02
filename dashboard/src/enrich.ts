import type { PlayerRow, Slate, PlayerForm } from "./types";

/**
 * Load-time enrichment: derive VAL, assign a stable model rank, and build the
 * rank/percentile lookups the flags and percentile bars read.
 *
 * Doing this once up front is what makes the player card instant — every
 * "rank 12 of 149" and every percentile bar is a map lookup, not a scan.
 */

export interface Player extends PlayerRow {
  /** Player name doubles as the id — unique within a field, and the same key
   *  the Python side uses for `form`, since there are no IDs in the schema. */
  id: string;
  /** P(top-20) per $1k of salary. A derived column that did NOT exist in the
   *  Streamlit app — added because salary-vs-probability is the axis actually
   *  being optimized. */
  VAL: number;
  /** 1-based, by descending P_TOP20. Shown in the card header and lineup slots.
   *  There is deliberately no rank COLUMN in the grid — P(top-20) is the rank. */
  rank: number;
  form: PlayerForm | null;
}

/** Metrics carrying rank + percentile lookups. Higher raw value = better for
 *  all of them, including SALARY (rank 1 = most expensive), which is what makes
 *  the underpriced/overpriced flag's rank comparison meaningful. */
export const METRICS = [
  "P_TOP20",
  "VAL",
  "SG_FORM",
  "SG_CH_SHRUNK",
  "CUT_PERCENTAGE",
  "LEVERAGE",
  "SALARY",
  "ott",
  "app",
  "arg",
  "putt",
] as const;

export type Metric = (typeof METRICS)[number];

export interface Field {
  players: Player[];
  byId: Map<string, Player>;
  /** rnk[metric][id] — 1-based, best first. */
  rnk: Record<Metric, Record<string, number>>;
  /** pct[metric][id] — 0..1, 1 = best. */
  pct: Record<Metric, Record<string, number>>;
  /** How many players actually had a value for this metric. Ranks read
   *  "rank 12 of N" against THIS, not against the field size — 68% of the
   *  field has no season stats, and "rank 12 of 149" would be a lie. */
  n: Record<Metric, number>;
  maxP20: number;
  meta: Slate["meta"];
}

function rawValue(p: Player, m: Metric): number | null {
  switch (m) {
    case "ott":
      return p.form?.phases?.ott ?? null;
    case "app":
      return p.form?.phases?.app ?? null;
    case "arg":
      return p.form?.phases?.arg ?? null;
    case "putt":
      return p.form?.phases?.putt ?? null;
    default: {
      const v = p[m as keyof PlayerRow];
      return typeof v === "number" && Number.isFinite(v) ? v : null;
    }
  }
}

export function metricValue(p: Player, m: Metric): number | null {
  return rawValue(p, m);
}

export function enrich(slate: Slate): Field {
  const players: Player[] = slate.players.map((row) => ({
    ...row,
    id: row.PLAYER,
    VAL: row.SALARY > 0 ? (row.P_TOP20 * 100) / (row.SALARY / 1000) : 0,
    rank: 0,
    form: slate.form?.[row.PLAYER] ?? null,
  }));

  // Stable model rank before anything else, so it never depends on grid sort.
  [...players]
    .sort((a, b) => b.P_TOP20 - a.P_TOP20)
    .forEach((p, i) => {
      p.rank = i + 1;
    });

  const rnk = {} as Field["rnk"];
  const pct = {} as Field["pct"];
  const n = {} as Field["n"];

  for (const m of METRICS) {
    const withValue = players
      .map((p) => ({ id: p.id, v: rawValue(p, m) }))
      .filter((x): x is { id: string; v: number } => x.v !== null)
      .sort((a, b) => b.v - a.v);

    const r: Record<string, number> = {};
    const q: Record<string, number> = {};
    const count = withValue.length;

    withValue.forEach((x, i) => {
      r[x.id] = i + 1;
      // 1 = best. Single-value edge case yields 1 rather than NaN.
      q[x.id] = count > 1 ? (count - 1 - i) / (count - 1) : 1;
    });

    rnk[m] = r;
    pct[m] = q;
    n[m] = count;
  }

  return {
    players,
    byId: new Map(players.map((p) => [p.id, p])),
    rnk,
    pct,
    n,
    maxP20: Math.max(...players.map((p) => p.P_TOP20), 0),
    meta: slate.meta,
  };
}
