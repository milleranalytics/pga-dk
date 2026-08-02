/**
 * The contract with utils/dashboard.py.
 *
 * Every field here is written by `export_dashboard()` in the notebook. If you
 * rename a column on the Python side, this file must change too — and the
 * compile error you get is the whole point of typing it. Untyped access would
 * surface a notebook-side rename as `undefined` inside a rendered flag message
 * ("Hot form: SG undefined/rd") rather than as a build failure.
 *
 * Column names are SCREAMING_CASE inside `players` because those rows are the
 * notebook's `export_df` verbatim — the same 14 columns as
 * data/current_week_export.csv. Everything computed for the dashboard alone
 * uses snake_case, so the boundary between "the model's numbers" and "the
 * dashboard's numbers" stays visible at a glance.
 */

export interface SlateMeta {
  tournament: string;
  course: string;
  ending_date: string; // YYYY-MM-DD
  season: number;
  field_size: number;
  generated_at: string; // ISO 8601
  cap: number; // DraftKings salary cap, 50000
  roster: number; // roster size, 6
}

/** One row of the notebook's export_df. Unchanged from current_week_export.csv. */
export interface PlayerRow {
  PLAYER: string;
  SALARY: number;
  P_TOP20: number; // 0–1 probability
  SCORE: number; // rank blend, 1 = best
  MODEL_SCORE: number;
  ODDS_SHARE: number;
  LEVERAGE: number; // signed: model view minus Vegas view
  VEGAS_ODDS: number; // numerator of fractional odds (11 => "11/1")
  SG_FORM: number;
  PCT_FORM_SHRUNK: number;
  SG_CH_SHRUNK: number; // exactly 0 means NO course history, not neutral
  CUT_PERCENTAGE: number; // 0–100
  FEDEX_CUP_POINTS: number;
  OWGR_RANK: number;
}

/** [date, sg] — a played round. Tuple, not an object: this is the bulk of the
 *  payload (~150 players x ~80 rounds) and object keys would roughly triple it. */
export type RoundPoint = [string, number];

export interface CourseHistory {
  course: string;
  ev: number; // events played
  avg: number; // average finish
  best: number; // best finish
  cut_pct: number; // 0–100
}

export interface EventResult {
  date: string; // YYYY-MM-DD
  tournament: string;
  finish: string; // "1", "T26", "CUT", "W/D"
  sg: number | null; // per-event SG, null when no rounds parsed
}

/** Season strokes-gained by phase, from the `stats` table. */
export interface Phases {
  ott: number | null;
  app: number | null;
  arg: number | null;
  putt: number | null;
  ott_rank: number | null;
  app_rank: number | null;
  arg_rank: number | null;
  putt_rank: number | null;
}

/** Per-player history. Phase 2 fills this; Phase 0 emits an empty map. */
export interface PlayerForm {
  rounds_12m: number;
  cuts_20: number;
  top20_20: number;
  streak: [number, "made" | "missed"] | null;
  phases: Phases;
  rounds: RoundPoint[];
  courses: CourseHistory[];
  results: EventResult[];
}

/** One graded past prediction, for the Prediction Tracker. Phase 2. */
export interface TrackerRow {
  date: string;
  tournament: string;
  player: string;
  p_top20: number;
  finish: number | null; // FINAL_POS, null when not yet graded
  hit: boolean | null; // finish <= 20
}

export interface Slate {
  meta: SlateMeta;
  players: PlayerRow[];
  /** Keyed by PLAYER, matching players[].PLAYER exactly (post name-mapping).
   *  A player with no history gets an entry with empty arrays, never a
   *  missing key — so lookups never need a null guard. */
  form: Record<string, PlayerForm>;
  tracker: TrackerRow[];
}

declare global {
  interface Window {
    SLATE?: Slate;
  }
}
