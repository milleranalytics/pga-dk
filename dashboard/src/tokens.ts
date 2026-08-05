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
  track: "#232931", // progress bar tracks             (was #1e232a)
  lineSoft: "#191e25", // row dividers                  (was #171b21)
  line: "#282e37", // section and panel borders         (was #232830)
  lineStrong: "#39404a", // button borders, header underline, scrollbar thumb (was #2e343d)
  axis: "#49515c", // chart zero lines, empty-state text (was #3d444f)
  // Rule 6 — the grey ramp is one step brighter than it was, all the way down.
  // The model is Windows 11 Settings: a white title with a description under it
  // that is DARKER BUT NOT BY MUCH. text is the title (bold names); text2 is
  // that description, and carries every directionless number — salary, odds,
  // CUT9M, OWGR. The old ramp put too much air between the two.
  text: "#e8eaed", // primary — names, headings
  text2: "#d4d8de", // numeric secondary               (was #c8ccd2)
  muted: "#a3aab4", // labels                          (was #8b929c)
  dim: "#767e88", // tertiary labels, ranks, neutral (directionless) bars (was #5f666f)
  dimmer: "#5d646d", // footnotes, null-ish values     (was #4c525a)
  // Rule 1 — the only two value hues. The Soft variants are gone: SG:F in the
  // grid and the SG-by-phase bars are the same statement and now the same red.
  //
  // Green is WinUI's dark-theme SystemFillColorSuccess, taken as-is — it is the
  // "Connected" dot in Windows Settings, and it is saturated without being neon,
  // which is what the earlier hand-mixed mint was not.
  //
  // Red is NOT WinUI's SystemFillColorCritical (#ff99a4). That colour sits at
  // hue 353 and reads pink at grid density. Red here is held at hue ~4 — a true
  // red — and given as much depth as the contrast budget allows.
  //
  // The budget is the constraint worth understanding before touching either
  // value: matching green's 8.7:1 with a red REQUIRES going pale, because red's
  // luminance is dominated by a channel worth only 0.21 of the total against
  // green's 0.72. An equal-contrast red is a pink one; there is no deep red at
  // 8.7:1. So red is pinned at 7.5:1 — close enough that neither column
  // out-weighs the other in a dense grid, deep enough to still read as red. The
  // hue tells you the direction; it must never imply the magnitude.
  green: "#6ccb5f", // GOOD, everywhere    (was #8ccfa4, #6fae8a, #57d98a)
  red: "#ff7b72", // BAD, everywhere     (was #ff99a4, #e29088, #c9736b)
  // Rule 3 — app state, not data. WinUI's SystemFillColorCaution is #fce100,
  // a pure yellow that would out-shout both value hues for a rarer message, so
  // amber is instead matched to the pair's luminance (~9.3:1) by hand.
  amber: "#eab453", // caution, high exposure    (was #e0b46e, #cfa059, #e6b053)
  // Rule 2 — the lineup and its controls. This is literally the Windows 11
  // dark-theme accent (SystemAccentColorLight2, the toggle and primary-button
  // azure), not the old 213° slate blue: it separates cleanly from the greens
  // and reads as an interface colour rather than a data colour. Brighter than
  // the value hues on purpose — it marks a decision you made, which should
  // out-rank any single number on the row.
  blue: "#4cc2ff", // lineup membership, primary action (was #57bdf0, #6f9fd8)
  // Rule 5 — focus is grey. It was blue, which is now spoken for.
  selectBg: "#1b2028", // focused row, active tab   (was #1a1e25, #1b2430)
  focusEdge: "#d4d8de", // 2px inset edge on the focused row (= text2)
  // Rule 2 — lineup membership reads blue in every place it appears: the grid
  // row, the saved card. Retinted to the azure accent above.
  lineupBg: "#122733", // in-lineup row, active saved card (was #14262f, #16202e)
  excludeBg: "#181113", // excluded row                    (was #1a1113)
  // Rule 4 — chart marks have no direction, so they have no hue. Raw rounds are
  // the dim ramp; the rolling-form line is brighter because it is the summary
  // you are meant to read, not a better kind of value. The line used to be
  // green, which claimed "up = good" on a chart whose whole point is the spread.
  scatter: "#a3aab4", // chart data points        (= muted; was blue)
  trend: "#d4d8de", // rolling-average line       (= text2; was green)
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

/**
 * Rule 1 + rule 4 combined: the ramp for a metric that has BOTH an ordering and
 * a verdict at each end. Used by P(top-20) and VAL — the grid column and the
 * card's stat tile both call this, so the two cannot drift apart.
 *
 *      pct ≥ 0.90  green    the genuine top of the field
 *      pct ≥ 0.80  text     strong                    ┐
 *      pct ≥ 0.45  text2    above the middle          ├ identical to tier()
 *      pct ≥ 0.15  muted    below it                  ┘
 *      else        red      the bottom of the field
 *      undefined   dimmer   never measured — not the same as "measured, worst"
 *
 * This is `tier()` with exactly two edits: the top band is SPLIT at 0.90 so the
 * very best of the field goes green, and the bottom band is RECOLOURED red.
 * Every other breakpoint is shared, so a verdict column and a directionless one
 * change shade at the same percentiles and a row reads across cleanly — at
 * pct ≥ 0.80 every column in the grid is the brightest white unless it has
 * earned green.
 *
 * FIVE bands, not four, and the fifth is the point. P(top-20) and VAL correlate
 * hard (VAL is roughly P(top-20) per dollar), so a coarse ramp collapses them
 * into the same blocks and the VAL column stops saying anything the sort order
 * has not already said. The extra edge gives the two columns more places to
 * genuinely disagree — which is the signal worth having: a player whose VAL band
 * beats his P(top-20) band is one the salary is underrating.
 *
 * It replaces `p20Color` and `valColor`, whose green thresholds differed by 0.05
 * (0.80 vs 0.85). That gap exposed tier()'s brightest band in VAL's [0.80, 0.85)
 * sliver, a shade P(top-20)'s green covered and could never show — so the two
 * columns did disagree more often, but for a reason that carried no meaning.
 * The disagreement is preserved here and now tracks real percentile differences.
 */
export function rankColor(pct: number | undefined): string {
  if (pct === undefined) return c.dimmer;
  if (pct >= 0.9) return c.green;
  if (pct >= 0.8) return c.text;
  if (pct >= 0.45) return c.text2;
  if (pct >= 0.15) return c.muted;
  return c.red;
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
