/**
 * Design tokens from README.md ("Design Tokens"). Nothing in src/ should
 * hardcode a hex once this file exists — the handoff's rule, and the thing that
 * keeps later screens consistent without redesigning each one.
 */

/**
 * THE COLOUR RULES. Four hues, one job each. If a change needs a fifth, the
 * change is wrong.
 *
 *  1. GREEN = good. RED = bad. Exactly ONE of each, `c.green` and `c.red`, at
 *     one weight, in every context — grid cell, card number, bar, chart mark,
 *     dot. There is no "soft" variant: two greens meant the eye had to decide
 *     whether the difference carried information (it never did).
 *     Only values with a DIRECTION earn a hue. +0.42 SG is good; 11/1 odds and
 *     a 0.83 volatility are just large.
 *
 *  2. BLUE = the lineup you are building, and the controls that act on it.
 *     In-lineup row shading, the ＋ button, the lineup rail's active card, the
 *     Optimize button, form controls. Blue never means "good" and never
 *     encodes a value — it means "this is yours".
 *     Corollary: hue no longer marks lineup membership, which frees green to
 *     mean only one thing. LOCK stays green and EXCLUDE stays red, because
 *     those two ARE good/bad verdicts on a player.
 *
 *  3. AMBER = caution about the app's own state — over-exposure, a degraded
 *     sync, a truncated result set. Never a data value. If a number is simply
 *     bad, it is red.
 *
 *  4. NO HUE = everything else, tiered by LIGHTNESS instead:
 *         text  → text2 → muted → dim → dimmer
 *         best     good    mid    weak   absent
 *     This is the `tier()` ramp below. Use it for any number that has an
 *     ordering but no verdict (odds, OWGR, cut rate, rounds played) and for
 *     chart marks that are neither good nor bad.
 *
 *  5. FOCUS ("the row I am looking at") is a neutral grey wash with a light
 *     edge — never blue, never green. It changes on every click, so it must not
 *     compete with the committed state (lineup) it sits on top of.
 *
 * Previous values are kept in comments so the old look can be diffed or
 * restored token-by-token without digging through git.
 */
export const c = {
  bg: "#0b0d10", // app background, scrollbar track, player-card column
  panel: "#0e1116", // top bar, cards, lineup rail
  surface: "#12151a", // grid header, stat cards, bar tracks, slots
  surfaceAlt: "#161a20", // input fields
  slotEmpty: "#101317", // empty lineup slots
  track: "#1e232a", // progress bar tracks
  lineSoft: "#171b21", // row dividers
  line: "#232830", // section and panel borders
  lineStrong: "#2e343d", // button borders, header underline, scrollbar thumb
  axis: "#3d444f", // chart zero lines, empty-state text
  text: "#e8eaed", // primary
  text2: "#c8ccd2", // numeric secondary
  muted: "#8b929c", // labels
  dim: "#5f666f", // tertiary labels, ranks, neutral (directionless) bars
  dimmer: "#4c525a", // footnotes, null-ish values
  // Rule 1 — the only two value hues. The Soft variants are gone: SG:F in the
  // grid and the SG-by-phase bars are the same statement and now the same red.
  green: "#6fae8a", // GOOD, everywhere    (was #57d98a; greenSoft #9fd8b4 removed)
  red: "#c9736b", // BAD, everywhere     (was #e0655c; redSoft #d09a95 removed)
  // Rule 3 — app state, not data.
  amber: "#cfa059", // caution, high exposure          (was #e6b053)
  // Rule 2 — the lineup and its controls. Brighter than the value hues on
  // purpose: it marks a decision you made, which should out-rank any single
  // number on the row.
  blue: "#6f9fd8", // lineup membership, primary action (was #6aa9f0)
  // Rule 5 — focus is grey. It was blue, which is now spoken for.
  selectBg: "#1a1e25", // focused row, active tab    (was #1b2430, then #171d26)
  focusEdge: "#c8ccd2", // 2px inset edge on the focused row (= text2)
  // Rule 2 — lineup membership reads blue in every place it appears: the grid
  // row, the saved card. Same relationship the old green pair had.
  lineupBg: "#16202e", // in-lineup row, active saved card (was #13201a)
  excludeBg: "#181113", // excluded row                    (was #1a1113)
  // Rule 4 — chart marks have no direction, so they have no hue. Raw rounds are
  // the dim ramp; the rolling-form line is brighter because it is the summary
  // you are meant to read, not a better kind of value. The line used to be
  // green, which claimed "up = good" on a chart whose whole point is the spread.
  scatter: "#8b929c", // chart data points          (= muted; was blue)
  trend: "#c8ccd2", // rolling-average line       (= text2; was green)
} as const;

// --- semantic colour helpers ------------------------------------------------
// Every value-to-colour decision lives here, so the grid cell and the card
// number for the SAME metric cannot drift apart — that was point 4 of the
// second review round: VAL agreed with its column and P(top-20) did not.

/** Rule 4: the no-hue lightness ramp, keyed on field percentile (1 = best).
 *  `undefined` means the metric was never measured for this player, which is a
 *  different state from "measured and worst". */
export function tier(pct: number | undefined): string {
  if (pct === undefined) return c.dimmer;
  if (pct >= 0.8) return c.text;
  if (pct >= 0.45) return c.text2;
  if (pct >= 0.15) return c.muted;
  return c.dim;
}

/** Rule 1: a signed value — the sign IS the verdict. */
export function dirColor(v: number | null | undefined): string {
  if (v === null || v === undefined) return c.dimmer;
  return v >= 0 ? c.green : c.red;
}

/** P(top-20): green only for the genuine top of the field, then the grey ramp.
 *  Used by the grid column AND the card's stat tile. */
export function p20Color(pct: number | undefined): string {
  if (pct !== undefined && pct >= 0.8) return c.green;
  return tier(pct);
}

/** Value per $1k: green at the top, red at the bottom — unlike P(top-20) it is
 *  genuinely two-sided, because a bad VAL is an actively bad roster spend. */
export function valColor(pct: number | undefined): string {
  if (pct === undefined) return c.dimmer;
  if (pct >= 0.85) return c.green;
  if (pct <= 0.15) return c.red;
  return tier(pct);
}

/** Two families only. Mono for EVERY number and every uppercase micro-label;
 *  Sans for names, headings, and sentences. That split is the core of the look —
 *  do not substitute Inter or Roboto. */
export const font = {
  sans: '"IBM Plex Sans", system-ui, sans-serif',
  mono: '"IBM Plex Mono", ui-monospace, monospace',
} as const;

/** Roster rules. Mirrored from meta at runtime; these are the fallbacks. */
export const DK = {
  cap: 50000,
  roster: 6,
} as const;
