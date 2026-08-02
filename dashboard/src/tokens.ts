/**
 * Design tokens from README.md ("Design Tokens"). Nothing in src/ should
 * hardcode a hex once this file exists — the handoff's rule, and the thing that
 * keeps later screens consistent without redesigning each one.
 */

export const c = {
  bg: "#0b0d10", // app background, scrollbar track
  panel: "#0e1116", // top bar, player card, lineup rail
  surface: "#12151a", // grid header, stat cards, slots, saved cards
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
  dim: "#5f666f", // tertiary labels, ranks
  dimmer: "#4c525a", // footnotes, null-ish values
  green: "#57d98a", // positive, primary action, brand
  greenSoft: "#9fd8b4", // positive numerics in dense rows
  red: "#e0655c", // negative, exclude
  redSoft: "#d09a95", // negative numerics in dense rows
  amber: "#e6b053", // caution, high exposure
  blue: "#6aa9f0", // selection, Vegas odds, informational
  selectBg: "#1b2430", // selected row, active tab, pinned course row
  lineupBg: "#13201a", // in-lineup row, active saved card
  excludeBg: "#1a1113", // excluded row
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
