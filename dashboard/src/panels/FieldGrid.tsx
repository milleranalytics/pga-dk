import { useEffect, useMemo, useRef, useState } from "react";
import type { CSSProperties } from "react";
import {
  c,
  font,
  MAX_EXPOSURE,
  nameColor,
  radius,
  rankColor,
  rowH,
  stretch,
  type as t,
  weight,
} from "../tokens";
import type { Field, Player } from "../enrich";
import { fmtSalary, fmtDelta, EM_DASH } from "../format";
import { BanIcon, Caret, LockIcon } from "../components/icons";

/**
 * The field grid.
 *
 * Structured around an explicit column-def array (key, label, width, align,
 * render, sort accessor) because the Results Browser is meant to be THIS
 * component with different column defs. It is deliberately not yet extracted
 * into a generic <DataGrid> — there is one consumer, and the right shape for
 * the abstraction is much easier to see with a second. The seam is the
 * `columns` array below; lifting it is a mechanical change.
 *
 * Layout note from the handoff, and it matters: header and rows share one
 * grid template and both carry min-width 906px so they scroll horizontally as
 * a single unit. The container is flex:1 0 620px — NOT shrinkable to nothing.
 * An earlier version let the grid be the only shrinkable child and it collapsed
 * to 188px at a narrow viewport, hiding ten of twelve columns.
 */

/**
 * P(TOP-20) lost its progress bar and 50px with it. The bar was scaled to the
 * field's best, so on a normal slate two-thirds of the column was a solid green
 * wall — the strongest visual signal on the screen, spent restating a number
 * printed 6px to its right. The value now takes rankColor(), which says the same
 * "who is at the top" in the space of the digits themselves.
 */
/**
 * ONE RAMP. Every numeric column here except SALARY and EXP is rankColor() on
 * that column's field percentile — see the rules block in tokens.ts. Before
 * this, six columns ran six private colour scales, so a green cell in ODDS and
 * a green cell in SG:F were not claiming the same thing and the row could not
 * be read across. Adding a column? It goes on the ramp unless it is one of the
 * three documented exceptions.
 */
/**
 * The action column lost its ＋ (add to lineup) button and 24px with it, and L
 * absorbed the job. Optimize rebuilds around LOCKS rather than around the
 * current build, so "in the lineup but not locked" is not a state the optimizer
 * preserves — ＋ and L had collapsed into the same instruction, and L is the one
 * that survives a re-optimize. L now RE-SOLVES on every press (see build.ts),
 * so it fills a slot on the spot and the optimizer works out who makes way when
 * the roster is already full. That is the whole hand-build gesture, in the
 * column where the player already is.
 */
/**
 * THE ACTION COLUMN IS NARROWER BY 12px, and that is the icons paying for
 * themselves. Two 20px outlined boxes plus a 3px gap wanted 60px of column;
 * two 18px icons with no box want 48px, and the twelve saved pixels go to the
 * PLAYER column, which is the one that actually runs out of room on a long
 * name.
 */
/**
 * THE HEADINGS WERE SHORTENED RATHER THAN THE COLUMNS WIDENED (owner, Sep 2026).
 *
 * One face means an uppercase micro-label in Archivo at `stretch.label` is wider
 * than the same label was in IBM Plex Mono, and two headings had outgrown the
 * numbers underneath them: "P(TOP-20)" wrapped to a second line at 62px and made
 * the header row 25px tall against every other cell's 11, and it had just been
 * given 78px to stop doing so — 16px spent on a caption, in a grid where the
 * PLAYER column is the one that actually runs out of room.
 *
 * So `P(TOP-20)` is `P20` and `CUT9M` is `CUT`, and 36px comes back. The rule
 * that makes this safe rather than cryptic: EVERY heading carries a `title`
 * naming what the column is in a sentence (see `columns` below), so the short
 * form is a label and the long form is one hover away. That is the same trade
 * the app already makes on the L/X icons — the terse thing on screen, the
 * sentence where you are already pointing when you ask.
 *
 * `P20` RATHER THAN `P(T-20)`, and the reason is not width: the app was calling
 * this one number three things at once — `P(T-20)` in the grid, `TOTAL P(T-20)`
 * on the rail, `P20` on a saved card. Three names for one quantity is a worse
 * problem than a long one, so the shortest of the three won and the other two
 * were changed to match. It also happens to fit.
 *
 * The heading cells carry `whiteSpace: nowrap` so the next label that outgrows
 * its column overflows where it can be SEEN rather than silently reflowing the
 * row, which is how the first one went unnoticed.
 */
/**
 * THE WIDTHS LIVE IN `columns` NOW, and TEMPLATE is derived from them. That is
 * not tidiness: FREEZING A BLOCK NEEDS EVERY WIDTH TO ITS LEFT AS A NUMBER.
 * A sticky cell is offset by `left: <sum of the widths before it>`, so the
 * template and those offsets have to be the same numbers or the frozen columns
 * drift apart from the cells they are meant to sit over — silently, and only
 * once you scroll. One table, two consumers.
 */
const TEMPLATE = () => columns.map((col) => `${col.w}px`).join(" ");
const MIN_WIDTH = () => columns.reduce((a, col) => a + col.w, 0);

/**
 * HOW MANY COLUMNS STAY PUT WHILE THE REST SCROLL — the first four: the L/X
 * icons, PLAYER, SALARY and P20 (owner, Sep 2026).
 *
 * WHY THOSE FOUR AND NOT TWO. Identity is the usual reason to freeze a column
 * — a number eight columns right is useless without a name attached — but the
 * owner's is stronger: P20 is the column the whole grid is read AGAINST. "The
 * rest are just for color." So the frozen block is the thing you compare with
 * (who, what he costs, what he is worth) and the scrolling half is the evidence
 * for it, which is the natural seam rather than an arbitrary count.
 *
 * It only does anything below ~1600px, where the grid runs out of room and
 * scrolls. Above that every column is on screen and the sticky offsets are
 * simply never exercised — which is the right way round: it costs nothing when
 * it is not needed and is there on a laptop when it is.
 */
const FROZEN = 4;

interface Column {
  key: SortKey | null; // null = not sortable (the action column)
  label: string;
  align: "left" | "right";
  /** Track width in px. The template and the frozen offsets both read it. */
  w: number;
  /**
   * WHAT THE COLUMN IS, in a sentence. Not optional in spirit: a heading here
   * is four to eight characters and several are abbreviations of abbreviations,
   * so the sentence is the only place the column is actually defined. It is
   * what lets `P(T-20)` and `CUT` be that short.
   *
   * Say what it MEASURES and over what WINDOW — the window is the half that is
   * never guessable and the half that has already caused one bug here (CUT9M vs
   * the card's last-20-starts cut rate, which disagree for 119 of 146 players).
   */
  tip?: string;
}

const columns: Column[] = [
  // Deliberately unlabelled. "L X" was a heading that repeated, in the same
  // glyphs and the same order, the two buttons sitting directly under it — it
  // could only ever tell you what the buttons already said.
  { key: null, label: "", align: "left", w: 48 },
  {
    // 178px, AND IT WAS `minmax(150px, 1fr)` (owner, Sep 2026: "quite a bit of
    // dead space to the right of even the longest player"). `1fr` does not mean
    // "as much as a name needs", it means "every pixel nobody else claimed" —
    // so the column grew with the window and held a 110px name in 233px of box.
    //
    // MEASURED IN THE FACE THAT RENDERS IT, not counted in characters: at
    // Archivo 13/600 the widest name in a typical field is ~110px, and the
    // widest the tour actually produces are "Christiaan Bezuidenhout" (148px)
    // and "Adrien Dumont de Chassart" (165px). 178 = 148 + the 10px indent +
    // room to breathe, so every ordinary long name fits outright and only that
    // 25-character outlier ellipsises — by seven pixels, still unmistakable,
    // and his full name is on the card a click away.
    //
    // A FIXED WIDTH IS ALSO WHAT LETS THE BLOCK FREEZE: see FROZEN_LEFT. The
    // leftover no longer goes to this column and simply ends the tracks early,
    // which is a trailing margin rather than a gap inside the data.
    key: "PLAYER",
    label: "PLAYER",
    align: "left",
    w: 178,
    tip: "Click a name to open his card.",
  },
  {
    key: "SALARY",
    label: "SALARY",
    align: "right",
    w: 80,
    // Exception 2 at the palette level, said again here: a price has no good
    // end, so it takes no ramp colour and the heading says why.
    tip: "DraftKings' price this week. Uncoloured — a price is the constraint you are spending, not a measure of the player.",
  },
  {
    key: "P_TOP20",
    label: "P20",
    align: "right",
    w: 52,
    tip: "P(TOP-20) — the model's probability, as a percentage, that he finishes in the top 20 this week. The objective the optimizer maximises.",
  },
  {
    key: "VAL",
    label: "VAL",
    align: "right",
    w: 62,
    tip: "Value: P(TOP-20) points per $1,000 of salary. The lineup's own figure is on the rail.",
  },
  {
    key: "LEVERAGE",
    label: "LEV",
    align: "right",
    w: 54,
    tip: "Leverage: the model's view minus the market's, in percentage points. Positive means the model likes him more than Vegas does.",
  },
  {
    key: "VEGAS_ODDS",
    label: "ODDS",
    align: "right",
    w: 56,
    tip: "Outright winner price, as the numerator of fractional odds — 11 is 11/1. Shorter is a better player.",
  },
  {
    key: "SG_FORM",
    label: "SG:F",
    align: "right",
    w: 60,
    tip: "Strokes gained — FORM. Recent per-round strokes gained, exponentially weighted so the last few starts count most.",
  },
  {
    key: "SG_CH_SHRUNK",
    label: "SG:C",
    align: "right",
    w: 60,
    tip: "Strokes gained — COURSE HISTORY at this week's venue, shrunk toward the field mean by how few rounds he has played here. Dim means never measured, not measured badly.",
  },
  // THE WINDOW USED TO BE IN THE LABEL, and it is in the tooltip now (owner,
  // Sep 2026). "CUT9M" was four characters of caption defending against one
  // confusion: the player card's FORM PROFILE shows a LAST-20-STARTS cut rate,
  // and the two disagree for 119 of 146 players, so an unlabelled "CUT" beside
  // a labelled "CUTS /20" reads as the same number twice. The sentence below
  // says the window in words and says it better, and the column gets its 8px
  // back. The defence is kept, not dropped — it moved to where you ask.
  {
    key: "CUT_PERCENTAGE",
    label: "CUT",
    align: "right",
    w: 46,
    tip: "Cut rate over the LAST 9 MONTHS, as a percentage of starts. Not the same number as the card's CUTS /20, which counts his last 20 starts however far back those go.",
  },
  {
    key: "OWGR_RANK",
    label: "OWGR",
    align: "right",
    w: 52,
    tip: "Official World Golf Ranking, this season. 1 is best; an em dash means he is unranked.",
  },
  {
    key: "EXP",
    label: "EXP",
    align: "right",
    w: 70,
    tip: "Exposure: the share of your SAVED lineups he appears in. Amber past 60%. This is a warning about your build, not a measure of the player, which is why it is the one column off the ramp.",
  },
];

/** Each frozen column's distance from the scroller's left edge — the running
 *  sum of the widths before it. Derived, for the reason above. */
const FROZEN_LEFT: number[] = columns
  .slice(0, FROZEN)
  .reduce<number[]>((acc, _col, i) => [...acc, i === 0 ? 0 : acc[i - 1] + columns[i - 1].w], []);

/**
 * The sticky props for column `i`, or nothing if it is not frozen.
 *
 * `background: "inherit"` IS THE WHOLE TRICK, and it replaces the cascade
 * juggling nfl-dk needs for the same effect. A sticky cell must be OPAQUE —
 * the scrolling columns pass underneath it — and the row's colour is four
 * different things (plain, in-lineup, excluded, focused) plus a hover. Rather
 * than restate all five per cell, each frozen cell inherits its parent's
 * COMPUTED background, so it is by construction the colour the row is wearing
 * at that moment, hover included. One declaration, every state, and no way for
 * a cell to disagree with its own row.
 *
 * That is why `.gridrow` now paints `--c-bg` rather than leaving the plain row
 * transparent: `inherit` on a transparent parent inherits transparency, and the
 * frozen cells would have the grid sliding through them.
 */
function frozen(i: number, body?: { edge?: string }): CSSProperties | undefined {
  if (i >= FROZEN) return undefined;

  /**
   * EVERY LINE THIS CELL DRAWS, IN ONE LIST — and it has to be one list,
   * because an inline `boxShadow` REPLACES what was there rather than adding to
   * it. Painted first-listed on top, which is what puts the two edges ABOVE the
   * row hairline instead of being notched by it once per row.
   */
  const shadows: string[] = [];

  // THE ROW'S LEFT EDGE — light when the row is being read, blue when the
  // player is in the lineup. It lives HERE rather than on the row (owner, Sep
  // 2026: "the entire left border should light up").
  //
  // An inset shadow paints under its element's own CHILDREN, so the moment this
  // cell became opaque it covered the edge everywhere except the few pixels
  // above and below its text — a solid 34px bar rendering as two stubs. Drawn
  // by the cell that was hiding it, it is whole again, and it gains something:
  // the cell is sticky, so the edge stays on screen when you scroll right,
  // which is the point of freezing the block in the first place.
  if (i === 0 && body?.edge) shadows.push(`inset 2px 0 0 ${body.edge}`);

  // THE EDGE OF THE BLOCK, drawn on the LAST frozen column rather than as a
  // border on the first scrolling one, so it travels with the block and
  // appears the moment anything has slid under it. `lineStrong` rather than
  // `line`: those separate columns that are all on screen together, this one
  // says content passes UNDERNEATH here.
  if (i === FROZEN - 1) shadows.push(`inset -1px 0 0 ${c.lineStrong}`);

  // THE ROW HAIRLINE, REDRAWN. The row paints it too, but under its children,
  // so these four opaque cells would erase it across the whole frozen block.
  // Listed LAST so the two edges above cross it unbroken. A header cell passes
  // no `body` and gets none — the header has its own, stronger, rule under it.
  if (body) shadows.push(`inset 0 -1px 0 ${c.lineSoft}`);

  return {
    position: "sticky",
    left: FROZEN_LEFT[i],
    zIndex: 1,
    background: "inherit",
    // FULL TRACK HEIGHT, NOT CONTENT HEIGHT, and this is the whole fix for both
    // broken lines. The row is `alignItems: center`, so a grid item is only as
    // tall as its own text — about 14px inside a 34px row. An opaque 14px cell
    // punches a 14px hole in whatever the row draws behind it and paints a 14px
    // STUB of whatever it draws itself, which is exactly the dashed divider and
    // the two-piece left edge that were reported.
    //
    // Stretching means the cell must center its own content, hence the three
    // lines below: `alignItems` for the vertical, `justifyContent` for the
    // horizontal its column already declared. `num()`'s `textAlign` is inert on
    // these four now and is left alone, because it is what every other cell in
    // the row says.
    alignSelf: "stretch",
    display: "flex",
    alignItems: "center",
    justifyContent: columns[i].align === "right" ? "flex-end" : "flex-start",
    boxShadow: shadows.length > 0 ? shadows.join(", ") : undefined,
  };
}

export type SortKey =
  | "PLAYER"
  | "SALARY"
  | "P_TOP20"
  | "VAL"
  | "LEVERAGE"
  | "VEGAS_ODDS"
  | "SG_FORM"
  | "SG_CH_SHRUNK"
  | "CUT_PERCENTAGE"
  | "OWGR_RANK"
  | "EXP";

/** Ascending feels natural for names and for a rank where 1 is best;
 *  everything else is "biggest first". */
const ASC_FIRST: SortKey[] = ["PLAYER", "OWGR_RANK"];

export function initialDir(key: SortKey): 1 | -1 {
  return ASC_FIRST.includes(key) ? 1 : -1;
}

export interface FieldGridProps {
  field: Field;
  query: string;
  sortKey: SortKey;
  sortDir: 1 | -1;
  onSort: (k: SortKey) => void;
  selected: string | null;
  onSelect: (id: string) => void;
  picks: string[];
  locks: Record<string, true>;
  excludes: Record<string, true>;
  exposure: Map<string, number>;
  savedCount: number;
  onToggleLock: (id: string) => void;
  onToggleExclude: (id: string) => void;
  /** Drops every lock and every exclusion, and with them the picks the locks
   *  were holding. Armed by a first press — see ClearConstraints. */
  onClearConstraints: () => void;
}

export default function FieldGrid(props: FieldGridProps) {
  const { field, query, sortKey, sortDir, exposure, savedCount } = props;

  const rows = useMemo(() => {
    const q = query.trim().toLowerCase();
    // Filter before sort, per the handoff.
    const filtered = q
      ? field.players.filter((p) => p.PLAYER.toLowerCase().includes(q))
      : field.players;

    const val = (p: Player): number | string | null => {
      if (sortKey === "PLAYER") return p.PLAYER.toLowerCase();
      if (sortKey === "EXP") return exposure.get(p.id) ?? 0;
      if (sortKey === "VAL") return p.VAL;
      return p[sortKey];
    };

    return [...filtered].sort((a, b) => {
      const av = val(a);
      const bv = val(b);
      // Missing values sink to the bottom in BOTH directions. Sorting by OWGR
      // is a way to find the ranked players; flipping the arrow should not
      // dredge up the twelve unranked Monday qualifiers instead.
      if (av === null || bv === null) {
        return av === bv ? 0 : av === null ? 1 : -1;
      }
      if (typeof av === "string" || typeof bv === "string") {
        return String(av).localeCompare(String(bv)) * sortDir;
      }
      return (av - bv) * sortDir;
    });
  }, [field.players, query, sortKey, sortDir, exposure]);

  return (
    <div
      style={{
        flex: "1 0 620px",
        minWidth: 620,
        borderRight: `1px solid ${c.line}`,
        overflow: "auto",
      }}
    >
      <div
        style={{
          display: "grid",
          gridTemplateColumns: TEMPLATE(),
          minWidth: MIN_WIDTH(),
          height: rowH.colHead,
          alignItems: "center",
          background: c.surface,
          borderBottom: `1px solid ${c.lineStrong}`,
          position: "sticky",
          top: 0,
          // ABOVE THE BODY'S FROZEN CELLS, which sit at z-index 1 inside rows
          // that create no stacking context of their own. This container DOES
          // create one, so everything inside it paints above them — which is
          // what stops a scrolled row sliding over its own headings.
          zIndex: 3,
          fontFamily: font.data,
          fontSize: t.colhead,
          fontWeight: weight.semi,
          // Width, not size, is what marks a heading as structure rather than
          // data — see tokens.stretch. There is no room above a 12px cell for a
          // larger heading, and there does not need to be.
          fontStretch: stretch.label,
          letterSpacing: "0.09em",
          color: c.muted,
        }}
      >
        {columns.map((col, i) => (
          <div
            key={col.key ?? "actions"}
            // A HEADING THAT SORTS IS A CONTROL, so it brightens on approach
            // like every other one. The unlabelled action column is not — it
            // holds CLR, which lights up on its own.
            className={col.key ? "dimhover" : undefined}
            onClick={col.key ? () => props.onSort(col.key as SortKey) : undefined}
            // WHAT IT MEASURES FIRST, how to sort it second. The definition is
            // the thing you actually came to the tooltip for; "click to sort"
            // is a reminder about a gesture the cursor has already promised.
            title={col.tip ? `${col.tip}\n\nClick to sort by this column.` : undefined}
            style={{
              textAlign: col.align,
              paddingLeft: col.align === "left" ? 10 : undefined,
              paddingRight: col.align === "right" ? 10 : undefined,
              cursor: col.key ? "pointer" : "default",
              userSelect: "none",
              // See TEMPLATE: a heading that outgrows its column must overflow
              // visibly rather than wrap and double the header's height.
              whiteSpace: "nowrap",
              // The SORTED heading is set inline so it does not dim back down
              // when the mouse crosses a different one.
              color: col.key === sortKey ? c.text2 : undefined,
              // A frozen HEADING is sticky in both axes at once: `top` from the
              // container above, `left` from here. `background: inherit` picks
              // up the header's own surface, so the scrolling headings pass
              // under it rather than through it.
              ...frozen(i),
            }}
          >
            {/* The action column's header is where CLR lives — the only place
                on the grid that acts on ALL rows rather than on one, which is
                exactly what a column header is for. */}
            {col.key === null ? (
              <ClearConstraints
                lockCount={Object.keys(props.locks).length}
                excludeCount={Object.keys(props.excludes).length}
                onConfirm={props.onClearConstraints}
              />
            ) : (
              /* THE ARROW IS DRAWN, NOT TYPED, and it is inline with the label,
                 which is exactly where a fallback face does damage: `▲` and
                 `▼` are not in Archivo, so the glyph arrived from whatever the
                 system offered and dragged the heading's baseline with it. See
                 components/icons.tsx. Neutral colour: which column is sorted is
                 UI state, not a verdict. */
              <span
                style={{
                  display: "inline-flex",
                  alignItems: "center",
                  gap: 4,
                  // The heading's own alignment is on the CELL; this inner box
                  // has to repeat it or a right-aligned column's label would
                  // start hugging the left edge of its own flex line.
                  justifyContent: col.align === "right" ? "flex-end" : "flex-start",
                  width: "100%",
                }}
              >
                {col.label}
                {col.key === sortKey && (
                  <Caret dir={sortDir === -1 ? "down" : "up"} size={8} color={c.text} />
                )}
              </span>
            )}
          </div>
        ))}
      </div>

      <div style={{ minWidth: MIN_WIDTH() }}>
        {rows.map((p) => (
          <Row key={p.id} p={p} {...props} savedCount={savedCount} />
        ))}
      </div>
    </div>
  );
}

function Row({
  p,
  field,
  selected,
  onSelect,
  picks,
  locks,
  excludes,
  exposure,
  savedCount,
  onToggleLock,
  onToggleExclude,
}: FieldGridProps & { p: Player }) {
  const isSelected = selected === p.id;
  const inLineup = picks.includes(p.id);
  const isExcluded = !!excludes[p.id];
  const isLocked = !!locks[p.id];

  /**
   * THE ROW'S STATE AS A NAME, NOT A COLOUR — and the colour is `index.css`'s
   * business now (owner-found regression, Sep 2026).
   *
   * It was an inline `backgroundColor`, which is the fourth time this app has
   * been bitten by the same rule: AN INLINE DECLARATION BEATS A STYLESHEET, so
   * `.gridrow:hover` matched, resolved its token, and painted nothing. Nobody
   * noticed while the plain row was TRANSPARENT and the hover had nothing to
   * override; freezing the block made every row opaque, and the hover died the
   * moment it did.
   *
   * Naming the state instead lets the cascade express the whole rule, which it
   * does better than a ternary could: `:hover` and the committed states carry
   * equal specificity, so SOURCE ORDER decides, and the committed rules come
   * last — a decision you made out-ranks where the pointer happens to be. A
   * plain row has no committed rule to match, so it lights up.
   *
   * The EDGE stays inline and stays independent of the background, so the two
   * compose rather than overwrite: a row in the lineup and being read is blue
   * with a light edge, which is exactly what it is.
   */
  const rowState = inLineup
    ? "lineup"
    : isExcluded
      ? "excluded"
      : isSelected
        ? "selected"
        : "";
  /**
   * THE LEFT EDGE — what you did to this player, in one 2px bar (owner, Sep
   * 2026: "locking the player should also make his left edge green... it's an
   * easy way to tell which players are locked", and excluding him should make
   * it red).
   *
   * THE ORDER IS BY HOW MUCH ELSE SAYS IT, not by importance. Every state below
   * has other cues; the edge goes to whichever has the FEWEST.
   *
   *   LOCKED — green, and it out-ranks blue because a lock IMPLIES the lineup:
   *     a locked player is in the build by definition, so blue could never tell
   *     the two apart. Its only other cue is an 18px padlock, in the same green
   *     so the bar and the icon agree.
   *   EXCLUDED — red. Mutually exclusive with a lock by construction, not by
   *     luck: `toggleLock` drops the exclusion and `toggleExclude` drops the
   *     lock (build.ts), so these two branches can never both be true and the
   *     order between them decides nothing.
   *   FOCUSED — light, and it LOSES to both of the above, which is the one
   *     trade here. Focus is the only state with a third cue of its own: the
   *     brightest name in the column (`nameColor`), plus `select-bg` when the
   *     row is not already washed. So the row you are reading still reads as
   *     read; a locked row simply keeps saying it is locked while you read it,
   *     which is what you asked the lock for.
   *   IN LINEUP — blue, last, because the background already says it.
   *
   * Drawn by `frozen(0)`, not by the row — see the note there.
   */
  const edge = isLocked
    ? c.green
    : isExcluded
      ? c.red
      : isSelected
        ? c.focusEdge
        : inLineup
          ? c.blue
          : undefined;

  const p20pct = field.pct.P_TOP20[p.id];
  const p20col = rankColor(p20pct);
  const exp = exposure.get(p.id) ?? 0;

  /**
   * THE EXPOSURE TONE, AND THE ONE TEST OF IT. Null means "nothing to warn
   * about", which the two call sites below turn into their own resting colour
   * — they are deliberately NOT the same colour (a fill wants to be quieter
   * than the figure beside it), and that is exactly why the THRESHOLD has to be
   * one expression rather than two.
   */
  const expTone = exp >= 100 ? c.red : exp >= MAX_EXPOSURE ? c.amber : null;

  return (
    <div
      // `.gridrow` carries the hover wash AND reveals the two action icons —
      // both in CSS rather than React state, because tracking the hovered row
      // here would re-render every row in the field on each mouse move. The
      // COMMITTED backgrounds below are inline, which beats the CSS rule, so
      // pointing at an in-lineup player never makes him stop looking like one.
      className="gridrow"
      data-player-id={p.id}
      data-row-state={rowState}
      onClick={() => onSelect(p.id)}
      style={{
        display: "grid",
        gridTemplateColumns: TEMPLATE(),
        alignItems: "center",
        height: rowH.body,
        // THE HAIRLINE IS AN INSET SHADOW, NOT A BORDER, and the difference is
        // the one pixel the edges were losing. A border eats into the content
        // box, so the grid tracks would be 33px tall inside a 34px row and
        // every stretched frozen cell would stop 1px short — putting a gap in
        // the divider and in the left edge once per row, 34px apart, which is
        // the thing that would still look broken after the stretch. As a shadow
        // the tracks are the full 34px, the cells fill them, and the frozen
        // cells redraw this same line themselves (see `frozen`) so nothing is
        // lost underneath them. Row height is unchanged: `box-sizing:
        // border-box` was already counting the border inside the 34.
        //
        // The row no longer draws the selected/in-lineup EDGE. It could not:
        // an inset shadow paints beneath the element's children, and four of
        // this row's children are opaque now. `frozen(0)` has it.
        boxShadow: `inset 0 -1px 0 ${c.lineSoft}`,
        fontFamily: font.data,
        fontSize: t.data,
        cursor: "pointer",
      }}
    >
      <div className="lx" style={{ paddingLeft: 8, ...frozen(0, { edge }) }}>
        <MiniBtn
          on={!!locks[p.id]}
          onClick={() => onToggleLock(p.id)}
          tone="good"
          title={
            locks[p.id]
              ? `LOCKED — every solve keeps ${p.PLAYER}. Click to unlock and re-solve.`
              : `Lock ${p.PLAYER} into the lineup. Every solve keeps him; the optimizer works out who makes way.`
          }
        />
        <MiniBtn
          on={isExcluded}
          onClick={() => onToggleExclude(p.id)}
          tone="bad"
          title={
            isExcluded
              ? `EXCLUDED — no solve will use ${p.PLAYER}. Click to put him back in the pool.`
              : `Exclude ${p.PLAYER}. No Optimize or Gen will use him until you click again.`
          }
        />
      </div>

      <div
        // The name is the click target for the player card, so it looks like
        // one on approach — `.namecell` is a hairline underline, nothing more.
        className="namecell"
        style={{
          ...frozen(1, { edge }),
          fontFamily: font.sans,
          fontSize: t.body,
          // Names are the "title" in the Windows-Settings pairing the grey ramp
          // is modelled on: bold, with every number a step below. Which step is
          // nameColor's business — the selected row's name is the brightest in
          // the column, a third cue alongside the wash and the edge.
          fontWeight: weight.semi,
          paddingLeft: 10,
          color: nameColor({ selected: isSelected, excluded: isExcluded }),
          overflow: "hidden",
        }}
      >
        {/* THE NAME IS IN A SPAN because the cell is a flex container now (see
            `frozen`), and `text-overflow: ellipsis` does not reach flex items —
            it truncates the text of the box it is set on. `minWidth: 0` is the
            other half: a flex item will not shrink below its own content unless
            told to, so without it a long name would overflow the frozen block
            and sit on top of SALARY rather than ellipsising. The hover underline
            moved with it (`.namecell:hover > span`), which also sizes it to the
            name instead of to the whole 178px column. */}
        <span
          style={{
            minWidth: 0,
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
          }}
        >
          {p.PLAYER}
        </span>
      </div>

      {/* Exception 2 — no colour. The ramp means "nearer the good end of the
          field", and there is no good end here: an $11,200 price tag is not
          better than a $6,400 one, it is the constraint you are spending. */}
      <div style={{ ...num(c.text2), ...frozen(2, { edge }) }}>{fmtSalary(p.SALARY)}</div>

      {/* EVERY column below is rankColor(), and that uniformity is the feature:
          one ramp, keyed on rank in this field, so a colour means the same thing
          in ODDS as it does in SG:F and the row reads across in one pass. Each
          of these used to argue its own case — LEV had a hand-tuned ±2 dead
          band, SG:F and SG:C coloured by sign, ODDS/CUT9M/OWGR ran a separate
          grey-only ramp with different breakpoints. Six private scales meant a
          green cell in one column and a green cell in the next were not claiming
          the same thing, which is precisely what made the grid hard to scan. */}
      <div style={{ ...num(p20col), fontWeight: weight.medium, ...frozen(3, { edge }) }}>
        {(p.P_TOP20 * 100).toFixed(1)}
      </div>

      <div style={{ ...num(rankColor(field.pct.VAL[p.id])), fontWeight: weight.medium }}>
        {p.VAL.toFixed(2)}
      </div>
      <div style={num(rankColor(field.pct.LEVERAGE[p.id]))}>{fmtDelta(p.LEVERAGE, 1)}</div>
      <div style={num(rankColor(field.pct.VEGAS_ODDS[p.id]))}>{p.VEGAS_ODDS.toFixed(0)}</div>
      <div style={num(rankColor(field.pct.SG_FORM[p.id]))}>{fmtDelta(p.SG_FORM, 2)}</div>
      {/* SG:C needs no ch_window test here any more. enrich() already drops the
          unmeasured from the ranking (rawValue returns null when ch_window is
          false), so they have no percentile and the ramp renders them dimmer —
          absence, not "measured and worst". Keying on the window rather than on
          the value being 0 still matters and still happens, just one layer down:
          the export rounds to 2dp, so a player at exactly field average also
          reads 0.00 and must not be mistaken for one with no history. */}
      <div style={num(rankColor(field.pct.SG_CH_SHRUNK[p.id]))}>
        {fmtDelta(p.SG_CH_SHRUNK, 2)}
      </div>
      <div style={num(rankColor(field.pct.CUT_PERCENTAGE[p.id]))}>
        {p.CUT_PERCENTAGE.toFixed(0)}
      </div>
      <div style={num(rankColor(field.pct.OWGR_RANK[p.id]))}>
        {p.OWGR_RANK === null ? EM_DASH : p.OWGR_RANK.toFixed(0)}
      </div>
{/* EXPOSURE — a bar in the dead space to the LEFT of the number, ported
          from nfl-dk (owner, Sep 2026).

          WHY A PICTURE HERE AND NOWHERE ELSE IN THIS GRID. Every other column
          is a RANK, and the ramp already draws ranks. This one is a THRESHOLD:
          "am I over 60% on this man" is a yes/no you should be able to take at
          a glance down the column, and reading it out of two digits means
          comparing each against a number you have to hold in your head.

          THE BAR IS THE FRACTION OF YOUR SAVED SET, not a fraction of the
          ceiling. Scaled to MAX_EXPOSURE it would saturate at 60 and draw 60,
          80 and 100 identically — losing the distinction on exactly the rows
          the column exists to show. The threshold is carried by the COLOUR,
          which is what a colour is for.

          AMBER, NEVER THE RAMP (tokens rule 3, exception 1). Over-exposure is a
          warning about YOUR saved set, not a measurement of the player. Red at
          100% says something stronger and still not a verdict on him: he is in
          every lineup you have, so your set has no diversity left at all.

          ONE EXPRESSION FEEDS BOTH. The bar and the figure are one statement
          made twice, and a picture disagreeing with its own number about
          whether you are over the ceiling would read as a rendering hair rather
          than as the bug it is.

          A DASH FOR NOBODY, NOT "0%", and the track goes with its bar. With six
          lineups saved most of a 150-man field is in none of them, so an
          always-drawn track would put ~140 empty scales on screen — exactly the
          texture the L/X icons were hidden to avoid. Nothing shifts either way:
          the figure's width is RESERVED below, so the bar's left edge lands in
          the same place on every row and a three-digit "100%" cannot push it
          out of line. */}
      <div
        title={
          savedCount === 0
            ? "Nothing saved yet — press Optimize then Save, or Gen."
            : `In ${Math.round((exp / 100) * savedCount)} of your ${savedCount} saved lineups.`
        }
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "flex-end",
          gap: 5,
          paddingRight: 10,
          fontSize: t.chip,
          color: savedCount === 0 || exp === 0 ? c.axis : (expTone ?? c.text2),
        }}
      >
        {savedCount > 0 && exp > 0 && (
          <ExposureBar pct={exp} color={expTone ?? c.dim} />
        )}
        <span data-cell="EXP" style={{ width: EXP_NUM_W, flex: "none", textAlign: "right" }}>
          {savedCount === 0 || exp === 0 ? EM_DASH : `${exp.toFixed(0)}%`}
        </span>
      </div>
    </div>
  );
}

/** The widest the EXP figure ever gets — "100%". RESERVED, so the BAR's left
 *  edge is in the same place on every row: the cell is `flex-end`, so without
 *  this a three-digit figure pushes its bar further left than a two-digit one
 *  and the 100% rows stick out. */
const EXP_NUM_W = 28;

/**
 * The exposure bar. The TRACK TRAVELS WITH ITS BAR — a 17% stub with nothing
 * behind it is a mark rather than a reading, so the two are one component, and
 * the call site draws neither on a row with no exposure.
 */
function ExposureBar({ pct, color }: { pct: number; color: string }) {
  return (
    <span
      aria-hidden="true"
      style={{
        display: "inline-block",
        width: 20,
        height: 4,
        flex: "none",
        borderRadius: 2,
        background: c.line,
        overflow: "hidden",
      }}
    >
      <span
        style={{
          display: "block",
          height: "100%",
          width: `${Math.max(0, Math.min(100, pct))}%`,
          borderRadius: 2,
          background: color,
        }}
      />
    </span>
  );
}

function num(color: string): CSSProperties {
  return { textAlign: "right", paddingRight: 10, color };
}

/**
 * CLR — drop every lock and exclusion at once.
 *
 * INVISIBLE WHEN THERE IS NOTHING TO CLEAR. A permanent button in a column
 * header that is a no-op on most page loads is a control you have to read and
 * dismiss every time you look at the grid; appearing only once you have set a
 * constraint makes its presence the status report as well as the action, and
 * means the unlabelled column stays empty exactly as long as it is empty.
 *
 * It does NOT touch the lineup — see clearConstraints in build.ts. The roster
 * on the rail is still a valid, cap-legal one; it is simply unconstrained now.
 *
 * Two presses anyway. Not because it is destructive — it is not — but because
 * there is no undo for a set of locks you spent a few minutes choosing, and
 * re-finding five players in a 149-row grid is a real cost for a misclick.
 * Same arm-and-disarm shape as the rail's saved-lineup CLEAR ALL, so the two
 * bulk buttons in this app behave identically.
 */
function ClearConstraints({
  lockCount,
  excludeCount,
  onConfirm,
}: {
  lockCount: number;
  excludeCount: number;
  onConfirm: () => void;
}) {
  const [armed, setArmed] = useState(false);
  const timer = useRef<number | undefined>(undefined);

  // Cleared on unmount so a pending disarm cannot fire into a dead component
  // (a tab switch away from the slate view while armed).
  useEffect(() => () => window.clearTimeout(timer.current), []);

  const total = lockCount + excludeCount;
  // Disarm the moment there is nothing left to clear, so the button cannot
  // unmount while armed and come back armed.
  useEffect(() => {
    if (total === 0) setArmed(false);
  }, [total]);

  if (total === 0) return null;

  const parts = [
    lockCount > 0 ? `${lockCount} lock${lockCount === 1 ? "" : "s"}` : null,
    excludeCount > 0 ? `${excludeCount} exclusion${excludeCount === 1 ? "" : "s"}` : null,
  ].filter(Boolean);

  return (
    <button
      // `.quietbtn` owns the resting grey and the hover; the ARMED amber is set
      // inline below, which beats it — an armed button must not dim back down
      // under the mouse that is about to press it again.
      className="quietbtn"
      onClick={(e) => {
        e.stopPropagation();
        window.clearTimeout(timer.current);
        if (armed) {
          setArmed(false);
          onConfirm();
        } else {
          setArmed(true);
          timer.current = window.setTimeout(() => setArmed(false), 3000);
        }
      }}
      title={
        (armed ? "Click again to clear " : "Clear ") +
        parts.join(" and ") +
        ". The lineup itself is left alone." +
        (armed ? "" : " Asks once first.")
      }
      style={{
        border: "none",
        background: "transparent",
        padding: 0,
        fontFamily: font.data,
        fontSize: t.micro,
        fontWeight: weight.semi,
        fontStretch: stretch.label,
        letterSpacing: "0.08em",
        // Amber is rule 3 — a warning about your own state, not a data verdict.
        // Inline, so it beats `.quietbtn`'s hover.
        color: armed ? c.amber : undefined,
        cursor: "pointer",
        lineHeight: 1,
      }}
    >
      {armed ? `CLR ${total}?` : "CLR"}
    </button>
  );
}

/**
 * LOCK AND EXCLUDE — the two controls on every field row.
 *
 * ICONS, NOT LETTERS. "L" and "X" are letterforms sitting in a grid of numbers,
 * and at 10px the X was routinely read as a multiplication sign; a padlock and
 * a slashed circle say what they do without being read.
 *
 * NO OUTLINE, AND NO FILL. The button is invisible at rest and appears only on
 * the row under the cursor, or when it is SET — so by the time you can see it,
 * it has something to say, and the icon's own colour is already saying it. A
 * box around it was a second statement of the same fact, and the old
 * filled-when-on style went with it: green and red are verdicts here (tokens
 * rule 2), and a verdict is worth a stroke, not a slab.
 *
 * THE SHOW-ON-HOVER IS `.lx` IN index.css, NOT STATE HERE. Tracking the hovered
 * row in React would re-render the whole field on every mouse move, and
 * `opacity` keeps the cell's full width reserved so nothing shifts under the
 * cursor mid-aim.
 */
function MiniBtn({
  on,
  onClick,
  tone,
  title,
}: {
  on: boolean;
  onClick: () => void;
  /** What the active state MEANS, per the colour rules: `good` is a favourable
   *  verdict (green), `bad` an exclusion (red). */
  tone: "good" | "bad";
  title: string;
}) {
  return (
    <button
      type="button"
      // `.lx button` in index.css owns the resting colour, the hover and the
      // reveal — an inline colour could not be brightened by a :hover rule, and
      // `aria-pressed` is what keeps a SET control visible on an unhovered row.
      className={tone === "good" ? "lx-lock" : "lx-excl"}
      aria-pressed={on}
      title={title}
      onClick={(e) => {
        // Must not also select the row.
        e.stopPropagation();
        onClick();
      }}
      style={{
        width: 18,
        height: 18,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        border: 0,
        borderRadius: radius.sm,
        background: "transparent",
        padding: 0,
        cursor: "pointer",
        lineHeight: 1,
      }}
    >
      {tone === "good" ? <LockIcon /> : <BanIcon />}
    </button>
  );
}
