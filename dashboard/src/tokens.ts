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
 *  4. ONE RAMP, and it is `rankColor()`. Every measured number that has a
 *     position in the field is coloured by WHERE IN THE FIELD IT SITS —
 *     green for the top tenth, red for the bottom fifth, four steps of grey
 *     between. Not by its sign, not by a per-column threshold. A colour means
 *     the same thing in the ODDS column as in the SG:F column as on the
 *     player card, which is what makes a row scannable across.
 *     The exceptions are enumerated at `rankColor` and there are only three.
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
  // Rule 5a — THE BRIGHTEN-ON-APPROACH EDGE, one step above lineStrong, and the
  // one colour every control in the app reaches for when the mouse arrives.
  // It is a TOKEN rather than a literal repeated in five index.css rules —
  // `.chip`, `.actionbtn`, `.lineupname`, `.segbtn` and the scrollbar thumb all
  // read it through `var(--c-line-hover)`, so there is one statement of it.
  lineHover: "#4a525e",
  // THE BRIGHTEN-ON-APPROACH WASH FOR AN ELEMENT WHOSE RESTING COLOUR IS SET
  // INLINE — a saved lineup card, which is `surface` at rest and `lineupBg`
  // when it is the one loaded. `.actionbtn` can simply swap its background on
  // hover because it has ONE resting colour; a card has two, and an inline
  // background cannot be replaced by a stylesheet rule at all. So the hover is
  // a TRANSLUCENT LAYER over whichever colour the card is wearing, applied as
  // a `background-image` so it composites over the inline `background-color`
  // rather than fighting it. One rule, both states.
  //
  // Not a hue (rule 1): it is a step of light, matching the step `selectBg`
  // makes over `surface`, so a hovered card and a hovered button read as the
  // same gesture.
  hoverWash: "rgba(255, 255, 255, 0.05)",
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

/**
 * THE RAMP. Rank in field → colour, and the only ramp in the app.
 *
 *      pct ≥ 0.80    green    top fifth
 *      pct ≥ 0.60    text2    second fifth
 *      pct ≥ 0.40    muted    third fifth
 *      pct ≥ 0.20    dim      fourth fifth
 *      else          red      bottom fifth
 *      undefined     dimmer   never measured
 *
 * ONE BAND PER QUINTILE. Even spacing is the whole property being bought: the
 * rule states in a single sentence — green is the top fifth, red is the bottom
 * fifth, three greys between — and from that sentence every cell on the screen
 * is predictable without a lookup table. A band that is 10% wide while its
 * mirror is 20% cannot be stated that way, and each colour added past five
 * takes meaning away from the ones already there.
 *
 * The top fifth of a full 144-man field is the top ~29, which brackets the
 * top-20 finish the model is literally predicting — so green on P(TOP-20) marks
 * roughly the players in contention for the payout the column is about.
 *
 * `c.text` is deliberately NOT in the ramp. It is the player-name colour, and
 * the brightest thing in the palette; leaving it out means no number can ever
 * reach name brightness, so the row hierarchy (name first, then its numbers)
 * holds by construction rather than by everyone remembering it. Brightness
 * ranks; hue is a separate channel laid across it.
 *
 * WHY RANK RATHER THAN SIGN, everywhere in the grid. Sign-based colouring gave
 * every column its own private meaning — +0.42 SG was green while an equally
 * ordinary 45/1 price was grey, and the row could not be read across because no
 * two cells were measuring goodness on the same scale. Under one ramp a row of
 * greens is a genuinely elite player and a row that fades to red is genuinely
 * short of the field, in whatever the column happens to measure. The cost is
 * real and worth naming: a positive SG:F that ranks in the field's bottom fifth
 * now reads red. That is the correct statement for a lineup you are building
 * out of THIS field — beating the field average is not the bar when 149 players
 * are competing for six slots.
 *
 * `undefined` is a different state from "measured and worst": it means the
 * metric was never measured for this player, so he is absent from the ramp
 * rather than at the bottom of it. enrich() is what enforces this — a metric
 * with no value returns null from rawValue() and gets no percentile at all.
 *
 * THE ONLY THREE EXCEPTIONS, all of them documented at their call site:
 *   1. AMBER — a warning about the app's own state, not about the player.
 *      Over-exposure in the EXP column is the one live case. A threshold
 *      breach is not a rank, so it does not take a rank colour.
 *   2. NO COLOUR — a value with no good end. Salary is a price and a player's
 *      name is an identity; neither competes, so neither is ranked.
 *   3. SIGN — kept in the three places where the reading really is "did he gain
 *      or lose strokes": the Strokes gained bars, and the per-event SG figures
 *      in At this course and Recent results. Those are histories of what
 *      happened, not standings in this week's field, so there is no rank to
 *      colour by.
 */
export function rankColor(pct: number | undefined): string {
  if (pct === undefined) return c.dimmer;
  if (pct >= 0.8) return c.green;
  if (pct >= 0.6) return c.text2;
  if (pct >= 0.4) return c.muted;
  if (pct >= 0.2) return c.dim;
  return c.red;
}

/** Exception 3: a signed value where the sign IS the reading. */
export function dirColor(v: number | null | undefined): string {
  if (v === null || v === undefined) return c.dimmer;
  return v >= 0 ? c.green : c.red;
}

/**
 * A tournament FINISH on the same ramp, without a field percentile to feed it.
 *
 * A finish is already a rank — it just needs a denominator, and the population
 * it was earned against is the field that teed off, not the ~70 who made the
 * cut. FIELD_SIZE is the standard full-field entry list; a 156-player major
 * shifts every band by about one place, which is inside the width of a band.
 *
 * Bands land at: green ≤29, text2 ≤58, muted ≤86, dim ≤115, red beyond. A
 * top-20 finish therefore sits comfortably inside green rather than on its edge.
 *
 * A missed cut is the bottom of that field by definition, so it is red. A
 * withdrawal is not a result at all, so it is an absence.
 */
const FIELD_SIZE = 144;

export function finishColor(finish: number | null | undefined): string {
  if (finish === null || finish === undefined || !Number.isFinite(finish)) {
    return c.dimmer;
  }
  return rankColor(Math.max(0, (FIELD_SIZE - finish) / (FIELD_SIZE - 1)));
}

/**
 * THE PLAYER NAME, whose brightness is a third selection cue on top of the grey
 * wash and the light edge — the name of the row you are reading is the brightest
 * name in the column, and every other name rests one step down.
 *
 * Rule 2 of the palette still holds: a name is an identity, never a verdict, so
 * this is not the rank ramp and never takes a hue. It is one channel (lightness)
 * carrying one fact (which row is under the cursor), with `dim` reserved for the
 * committed state that outranks it.
 *
 *      excluded   dim      you took him out; that beats "I am looking at him"
 *      selected   text     the brightest thing in the grid, and the only one
 *      resting    NAME_REST
 *
 * SETTLED AT c.text2, AND THE RESTING LEVEL IS THE POINT — not the selection
 * cue. Read that before retuning: the step up on select measures 1.19:1
 * (luminance 0.684 → 0.821) and is, by the owner's own description, "hardly
 * noticeable". That is not a defect to be fixed by reaching for a darker rest.
 * It is a third cue behind two loud ones (the grey wash and the light edge),
 * and a faint third cue is the correct weight for a third cue.
 *
 * What the setting actually buys is the OTHER 148 rows. At c.text2 a resting
 * name is exactly the brightness of a second-fifth number, so the name column
 * stops out-ranking the data and rejoins it — "lets the data take more
 * relevance with the contrast being more similar". The grid is read for its
 * numbers; the names are how you address them.
 *
 * The alternative was measured and rejected: c.muted #a3aab4 gives 1.94:1 and
 * an unmistakable selection cue, but drops names below salary and quiets the
 * whole column past "a little" — at which point names stop being what you scan
 * by, which is the one job they cannot give up.
 */
const NAME_REST: string = c.text2;

/**
 * EXCLUSION IS NO LONGER HANDLED HERE (owner, Sep 2026: "making them all dim
 * the same amount seems better").
 *
 * It used to return `c.dim` for an excluded player, which dimmed the NAME by
 * one step while the eleven numbers beside it stayed at full strength — so an
 * excluded row was dim in one column and bright in the rest, and read as a
 * name that had gone quiet rather than as a player who is out.
 *
 * The whole row now carries `opacity` instead (`.gridrow[data-excluded]` in
 * index.css), which is the nfl-dk rule and is the only way to dim things by the
 * SAME amount: opacity MULTIPLIES whatever each cell resolved to, so twelve
 * different colours — the ramp, the salary grey, the amber exposure, the red
 * edge, the ban icon — all step back together and keep their relationships to
 * each other. Restating that as twelve dimmer tokens would be twelve chances to
 * get one wrong, and would flatten the ramp on exactly the rows where you still
 * want to see why you excluded him.
 *
 * So this function is about FOCUS only, and `selected` is its only input.
 */
export function nameColor(state: { selected?: boolean }): string {
  return state.selected ? c.text : NAME_REST;
}

/**
 * THE THREE FACES, AND WHY `data` IS NOT MONOSPACE ANY MORE (owner, Sep 2026).
 *
 * A grid needs equal DIGIT widths so `19.4` sits under `23.7`. It does not need
 * equal LETTER widths, which is what monospace also gives you and which is the
 * whole of what read as "terminal": a name, a label and a number all take the
 * same texture, so the page has one voice and no hierarchy inside a row.
 *
 * `font-variant-numeric: tabular-nums` buys the alignment on its own, from a
 * proportional face. It is set ONCE, on `body` in index.css, rather than at the
 * ~78 call sites that used to name `font.mono` — a per-site opt-in is a rule
 * that is only true where somebody remembered it.
 *
 * SO THE SPLIT SURVIVES AND ONLY THE FACES CHANGED. `data` still marks "this is
 * a number or a micro-label" and `sans` still marks "this is prose", which is
 * what lets either be repointed later without the other. They happen to be the
 * same family today; that is a fact about this design, not a reason to merge
 * the two names.
 *
 * `code` is the one place a real monospace is still correct — SQL on the Query
 * tab and the `<code>` runs in the empty-state panels, where column alignment
 * of LETTERS is the point. It is the only reason IBM Plex Mono is still
 * vendored; IBM Plex Sans is gone.
 *
 * Archivo Variable carries both a weight and a WIDTH axis (62-125%), which is
 * what `stretch` below spends: a dense grid has no room for a 24px heading over
 * an 11px number, so hierarchy is carried by width instead of size. Self-hosted
 * and inlined as a data URI like the Plex faces were — no network at runtime,
 * which is the offline / file:// story the whole architecture rests on.
 */
export const font = {
  sans: '"Archivo Variable", "Archivo", system-ui, sans-serif',
  /** Every number, and every uppercase micro-label. Tabular by the body rule. */
  data: '"Archivo Variable", "Archivo", system-ui, sans-serif',
  /** Real monospace: SQL, and `<code>` runs. Nothing else. */
  code: '"IBM Plex Mono", ui-monospace, monospace',
} as const;

/**
 * THE TYPE SCALE — the thing this app did not have, and the reason a design
 * change used to cost a hundred edits.
 *
 * Every `fontSize` in `src/` reads from here. The rule for picking one: name
 * the ROLE, never the pixel — a caller asking for `type.data` keeps working
 * when the grid gets a point bigger, and a caller asking for `12` does not.
 *
 * The steps are close together on purpose. A dense grid is read at one size
 * with the differences carried by weight, width and colour; a scale with big
 * jumps in it would invite exactly the 24px heading there is no room for.
 */
export const type = {
  /** Stat-card captions — the smallest thing that still has to be read. */
  micro: 9,
  /** Uppercase micro-labels: section headings, rail labels, sync stamp. */
  label: 9.5,
  /** Grid column headings. */
  colhead: 10,
  /** Chips, and the dense secondary numbers beside them (EXP, rank suffixes). */
  chip: 10.5,
  /** Secondary prose: meta lines, footnotes, the status line. */
  small: 11,
  /** THE GRID CELL. The number you are actually reading. */
  data: 12,
  /** Player names, tab labels, button labels. */
  name: 12.5,
  /** Sentences. */
  body: 13,
  /** A rail stat's value. */
  lead: 15,
  /** A panel's own heading — the tournament name. */
  head: 17,
  /** A stat card's value, the largest number in the app. */
  hero: 19,
} as const;

/**
 * WEIGHTS, LIMITED TO WHAT ARCHIVO IS ACTUALLY ASKED FOR.
 *
 * Archivo Variable interpolates continuously, so any value renders — but naming
 * four steps is what stops a fifth appearing next to a fourth and meaning
 * nothing. Weight marks the few columns you steer by, and only keeps working
 * while that list stays short.
 */
export const weight = {
  normal: 400,
  medium: 500,
  semi: 600,
  bold: 700,
} as const;

/**
 * WIDTH AS HIERARCHY. Archivo's `wdth` axis, and the only two values used.
 *
 * `label` is the slightly expanded setting every uppercase micro-label takes;
 * everything else is normal. Two values, because this is a channel for marking
 * ONE distinction (structure vs data) and a third setting would make it a
 * gradient nobody can read.
 */
export const stretch = {
  normal: "100%",
  label: "116%",
} as const;

/**
 * CORNERS — 6 / 5 / 3, tight rather than playful.
 *
 * `sm` is cell-level: a control inside a table row sits beside a dozen square
 * cells, and a rounder one reads as a sticker. `lg` is for a container that is
 * genuinely a separate object.
 */
export const radius = { sm: 3, md: 5, lg: 6 } as const;

/** THE GRID'S VERTICAL RHYTHM. The rail's heading sits on the grid's header
 *  row, so both read the same number rather than two paddings that happen to
 *  agree today. */
export const rowH = { body: 34, colHead: 30 } as const;

/**
 * THE TOKENS, HANDED TO THE STYLESHEET.
 *
 * `index.css` needs the palette too — a `:hover` cannot be written inline, so
 * every brighten-on-approach rule in this app lives in CSS, and a stylesheet
 * that held its own copies of nine hex values is a second statement of a rule
 * that drifts.
 *
 * A stylesheet cannot import a token, but it CAN read a custom property. So
 * this writes every token onto `:root` before React mounts, and `index.css`
 * reads `var(--c-line-strong)` rather than `#39404a`. One statement, in this
 * file, for both languages.
 *
 * THE ONE DELIBERATE DUPLICATE is `body { background }` in `index.css`, which
 * has to be a literal: it paints the frame BEFORE this module has run, and a
 * `var()` with nothing behind it yet is transparent — a white flash on every
 * load. It is annotated there as the exception.
 *
 * Called from `main.tsx` at import time rather than from a React effect, so the
 * properties exist before the first paint of the app itself.
 */
const kebab = (s: string) => s.replace(/[A-Z]/g, (m) => "-" + m.toLowerCase());

export function installTokens(el: HTMLElement = document.documentElement): void {
  for (const [k, v] of Object.entries(c)) el.style.setProperty(`--c-${kebab(k)}`, v);
  for (const [k, v] of Object.entries(radius)) el.style.setProperty(`--r-${k}`, `${v}px`);
  for (const [k, v] of Object.entries(type)) el.style.setProperty(`--t-${kebab(k)}`, `${v}px`);
  for (const [k, v] of Object.entries(weight)) el.style.setProperty(`--w-${k}`, String(v));
  el.style.setProperty("--font-sans", font.sans);
  el.style.setProperty("--font-data", font.data);
  el.style.setProperty("--font-code", font.code);
  el.style.setProperty("--stretch-label", stretch.label);
  el.style.setProperty("--row-h", `${rowH.body}px`);
}

/** Roster rules. Mirrored from meta at runtime; these are the fallbacks. */
export const DK = {
  cap: 50000,
  roster: 6,
} as const;

/**
 * THE EXPOSURE CEILING — the largest share of your saved set one player may
 * appear in, as a percentage.
 *
 * ONE STATEMENT, because it is read by two things that must agree: `generate()`
 * REFUSES to build a lineup that would push anyone past it, and the grid's EXP
 * column turns AMBER at it. Those are the rule and the warning about the rule.
 * It lived in App.tsx while the grid carried its own hardcoded `60`, so a
 * change to the generator's ceiling would have left the column quietly warning
 * at the wrong number — the kind of drift that reads as correct.
 */
export const MAX_EXPOSURE = 60;
