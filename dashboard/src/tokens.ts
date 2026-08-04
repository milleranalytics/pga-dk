/**
 * Design tokens from README.md ("Design Tokens"). Nothing in src/ should
 * hardcode a hex once this file exists — the handoff's rule, and the thing that
 * keeps later screens consistent without redesigning each one.
 */

/**
 * Colour rules, in priority order — the thing that keeps the palette from
 * drifting back into a rainbow:
 *  1. Hue is reserved for DIRECTION. Green = better, red = worse. Nothing else
 *     earns a hue. A number that has no good/bad direction (Vegas odds, rounds
 *     played, volatility) is text/muted/dim — its magnitude is already carried
 *     by the digits, and tiering by lightness (text → text2 → muted → dim)
 *     reads faster than tiering by colour.
 *  2. Green and red are muted to the same weight everywhere. The old #57d98a
 *     was ~2× the chroma of the SG:F / SG:C tints in the grid, so every green
 *     thing on screen pulled the eye harder than its information deserved.
 *     There is no "loud green" left to reach for.
 *  3. Amber is caution only (over-exposure, degraded state, warnings) — never
 *     a third data hue.
 *  4. Blue is UI state (the selected row) and the one chart series that has a
 *     legend saying what it is. It is not a value colour.
 *
 * Previous (bright) values are kept in comments so the old look can be diffed
 * or restored token-by-token without digging through git.
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
  green: "#6fae8a", // positive, primary action        (was #57d98a)
  greenSoft: "#8dbfa2", // positive numerics in dense rows (was #9fd8b4)
  red: "#c9736b", // negative, exclude               (was #e0655c)
  redSoft: "#c19089", // negative numerics in dense rows (was #d09a95)
  amber: "#cfa059", // caution, high exposure          (was #e6b053)
  blue: "#7f9dc4", // selection, legended chart series (was #6aa9f0)
  selectBg: "#171d26", // selected row, active tab        (was #1b2430)
  lineupBg: "#141d18", // in-lineup row, active saved card (was #13201a)
  excludeBg: "#181113", // excluded row                    (was #1a1113)
  // Chart marks reuse the palette rather than introducing their own hues.
  // The handoff shipped a muted blue (#5b7fb8) and salmon (#e0806c) inherited
  // from the Streamlit app, which read as a second, unrelated colour scheme.
  // Blue already means "data / informational" here and green already means
  // "the model", which is exactly what a raw round and a form line are.
  scatter: "#6aa9f0", // chart data points  (= blue)
  trend: "#57d98a", // chart rolling-average line (= green)
} as const;

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
