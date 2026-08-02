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
  "ttg",
] as const;

export type Metric = (typeof METRICS)[number];

/** Whether the SG-by-phase bars use one scale for both directions. See the
 *  block in enrich() where it is applied for the tradeoff. */
const SYMMETRIC_PHASE_SCALE = true;

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
  /** Bar scale for the SG-by-phase rows.
   *
   *  Computed once over EVERY player and ALL FOUR phases, not per player and
   *  not per phase. Two properties follow, and both are the point:
   *   - The scale does not move when you switch players, so toggling between
   *     two players is a direct visual comparison.
   *   - One scale across all four rows means a +0.5 driving bar and a +0.5
   *     putting bar are the same length.
   *  Positive and negative extents are tracked separately so the field's best
   *  reaches the right edge and its worst reaches the left, with the zero line
   *  placed wherever that implies — no dead space at either end. */
  phaseScale: { posMax: number; negMax: number; zeroAt: number };
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
    case "ttg":
      return p.form?.phases?.ttg ?? null;
    case "SG_CH_SHRUNK":
      // Unmeasured players export 0.0, which is a placeholder, not a score.
      // Ranking them would put 40 fake zeros in the middle of the distribution
      // and shift every percentile around them. Excluded here so ranks and
      // percentiles describe only the players who actually have a measurement
      // — and so "rank 12 of 109" is a true statement.
      return p.form?.ch_window === false ? null : p.SG_CH_SHRUNK;
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

  const PHASE_KEYS: Metric[] = ["ott", "app", "arg", "putt"];
  let posMax = 0;
  let negMax = 0;
  for (const p of players) {
    for (const k of PHASE_KEYS) {
      const v = rawValue(p, k);
      if (v === null) continue;
      if (v > posMax) posMax = v;
      if (-v > negMax) negMax = -v;
    }
  }
  // Guard the degenerate cases (no stats at all, or all one sign) so a bar
  // never divides by zero or collapses the track to a single edge.
  if (posMax === 0 && negMax === 0) {
    posMax = 1;
    negMax = 1;
  } else if (posMax === 0) {
    posMax = negMax * 0.1;
  } else if (negMax === 0) {
    negMax = posMax * 0.1;
  }

  if (SYMMETRIC_PHASE_SCALE) {
    // Same strokes-per-pixel on both sides, so bar LENGTH always means
    // magnitude. The field's SG distribution is left-skewed — the worst player
    // in a phase is typically further below average than the best is above —
    // so letting each side fill its own extent would render a -0.7 bar SHORTER
    // than a +0.7 bar. Flip this to false to trade that fidelity for using
    // every pixel on both sides.
    const m = Math.max(posMax, negMax);
    posMax = m;
    negMax = m;
  }

  return {
    players,
    byId: new Map(players.map((p) => [p.id, p])),
    rnk,
    pct,
    n,
    maxP20: Math.max(...players.map((p) => p.P_TOP20), 0),
    phaseScale: {
      posMax,
      negMax,
      zeroAt: (negMax / (negMax + posMax)) * 100,
    },
    meta: slate.meta,
  };
}
