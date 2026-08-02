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

/**
 * [date, sg, trend] — one played round.
 *
 * `trend` is the exponentially weighted mean of SG at that point, using the
 * model's own 100-day halflife (utils.features.SG_HALFLIFE_DAYS). It ships
 * precomputed rather than being derived in the browser because pandas'
 * time-based EWMA is not trivially reproducible in JS, and a near-miss
 * reimplementation would draw a form line that disagrees with SG_FORM.
 *
 * Tuple, not an object: this is the bulk of the payload (~150 players x ~110
 * rounds) and object keys would roughly triple it.
 */
export type RoundPoint = [string, number, number];

/** The player's record at THIS week's course. Null when they have never
 *  played it — which is a distinct state from "played it badly". */
export interface CourseHere {
  ev: number; // events played
  avg: number; // average finish (FINAL_POS is 90-filled for CUT/WD)
  best: number; // best finish
  cut_pct: number; // 0–100
}

export interface EventResult {
  date: string; // YYYY-MM-DD
  tournament: string;
  finish: string; // "1", "T26", "CUT", "W/D"
  sg: number | null; // per-event SG, null when no rounds parsed
}

/** Season strokes-gained, from the `stats` table. `ttg` is tee-to-green — the
 *  ott+app+arg composite, not a fifth phase, so it is excluded from the phase
 *  bars and carries its own flag. */
export interface Phases {
  ott: number | null;
  app: number | null;
  arg: number | null;
  putt: number | null;
  ttg: number | null;
  ott_rank: number | null;
  app_rank: number | null;
  arg_rank: number | null;
  putt_rank: number | null;
  ttg_rank: number | null;
}

/**
 * Per-player history. `phases` ships now; the rest lands in Phase 2 and is
 * optional until then — an absent field means "not published yet", a present
 * field with nulls means "no data for this player". The UI must not conflate
 * them, which is why these are optional rather than defaulted to zero.
 */
export interface PlayerForm {
  phases: Phases;
  rounds_12m?: number;
  cuts_20?: number;
  top20_20?: number;
  streak?: [number, "made" | "missed"] | null;
  rounds?: RoundPoint[];
  /** Aggregate at this week's course; null = never played it. */
  course_here?: CourseHere | null;
  /** Every visit to this week's course, most recent first. */
  course_events?: EventResult[];
  /** True when SG_CH_SHRUNK is a real measurement — the player has rounds at
   *  this course inside the model's 7-year window.
   *
   *  Do NOT infer this from SG_CH_SHRUNK === 0. The export rounds to 2
   *  decimals, so a player who performed at exactly field average also reads 0
   *  (Beau Hossler: -0.0045 over 6 events at Detroit), and an absent player
   *  reads 0 too because the feature is NaN-filled. The two are different
   *  states and only this flag separates them. */
  ch_window?: boolean;
  results?: EventResult[];
}

/** One graded past prediction, for the Prediction Tracker. */
export interface TrackerRow {
  date: string;
  tournament: string;
  player: string;
  p_top20: number;
  finish: number | null; // FINAL_POS, null when not yet graded
  pos: string | null; // raw POS — "T24", "CUT", "W/D"
  hit: boolean | null; // finish <= 20
}

/** Per-week track record. Selection of the top 15 happens in Python with the
 *  same nlargest() call as grade_predictions(), because P_TOP20 is logged to
 *  3 decimals and boundary ties are common. */
export interface WeekRow {
  date: string;
  tournament: string;
  players: number;
  graded: boolean;
  expected: number; // summed P_TOP20 of the top 15
  hits: number | null; // how many of those finished top-20
  cut_rate: number | null; // percent of the top 15 that made the cut
}

/** SG form across ALL active players, not just this week's field. */
export interface SgRankRow {
  rank: number;
  player: string;
  sg_form: number;
  rounds_12m: number;
  move: number | null; // rank change vs 30 days ago; positive = climbed
  spark: number[]; // last 20 rounds of SG
}

export interface CoursePlayer {
  player: string;
  sg_model: number | null; // SG_CH_SHRUNK — the exact model feature
  sg_raw: number; // plain unshrunk mean at the course
  rounds: number;
  events: number;
  avg_finish_pct: number; // 0 = won, 1 = last
  cut_pct: number;
  best: number;
  last_played: number;
}

export interface CourseTable {
  course: string;
  events: number;
  first_year: number | null;
  last_year: number | null;
  players: CoursePlayer[];
}

export interface Slate {
  meta: SlateMeta;
  players: PlayerRow[];
  /** Keyed by PLAYER, matching players[].PLAYER exactly (post name-mapping).
   *  A player with no history gets an entry with empty arrays, never a
   *  missing key — so lookups never need a null guard. */
  form: Record<string, PlayerForm>;
  tracker: TrackerRow[];
  weeks: WeekRow[];
  sg_rankings: SgRankRow[];
  /** Only THIS week's course is precomputed. Every course would roughly double
   *  the payload, and SG_CH_SHRUNK is a shrunk 7-year window that would have to
   *  be reimplemented in SQL to compute others in the browser — a port that
   *  could silently disagree with the feature the model trained on. Other
   *  venues stay reachable through the Results Browser. */
  course: CourseTable;
}

declare global {
  interface Window {
    SLATE?: Slate;
  }
}
