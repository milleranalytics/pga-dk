import { useEffect, useMemo, useRef, useState } from "react";
import type { CSSProperties } from "react";
import { c, font, radius, rankColor, nameColor, rowH, stretch, type as t, weight } from "../tokens";
import type { Field, Player } from "../enrich";
import { fmtSalary, fmtDelta, EM_DASH } from "../format";

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
 * P(TOP-20) IS THE ONE COLUMN WIDER THAN ITS NUMBERS NEED, and the heading is
 * why: the app has ONE face now, and an uppercase micro-label in Archivo at
 * `stretch.label` is wider than the same label was in IBM Plex Mono. At 62px
 * "P(TOP-20) ▼" wrapped to a second line and made the header row 25px tall
 * against every other cell's 11 — measured, not guessed, and the reason the
 * heading cells below carry `whiteSpace: nowrap`: a label that no longer fits
 * must overflow where it can be SEEN rather than silently reflow the row.
 */
const TEMPLATE =
  "48px minmax(150px,1fr) 80px 78px 62px 54px 56px 60px 60px 54px 52px 62px";
const MIN_WIDTH = 836;

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

interface Column {
  key: SortKey | null; // null = not sortable (the action column)
  label: string;
  align: "left" | "right";
}

const columns: Column[] = [
  // Deliberately unlabelled. "L X" was a heading that repeated, in the same
  // glyphs and the same order, the two buttons sitting directly under it — it
  // could only ever tell you what the buttons already said.
  { key: null, label: "", align: "left" },
  { key: "PLAYER", label: "PLAYER", align: "left" },
  { key: "SALARY", label: "SALARY", align: "right" },
  { key: "P_TOP20", label: "P(TOP-20)", align: "right" },
  { key: "VAL", label: "VAL", align: "right" },
  { key: "LEVERAGE", label: "LEV", align: "right" },
  { key: "VEGAS_ODDS", label: "ODDS", align: "right" },
  { key: "SG_FORM", label: "SG:F", align: "right" },
  { key: "SG_CH_SHRUNK", label: "SG:C", align: "right" },
  // Window in the label: the card's FORM PROFILE shows a last-20-starts cut
  // rate, and the two disagree for 119 of 146 players. An unlabelled "CUT"
  // beside a labelled "CUTS /20" reads as the same number twice.
  { key: "CUT_PERCENTAGE", label: "CUT9M", align: "right" },
  { key: "OWGR_RANK", label: "OWGR", align: "right" },
  { key: "EXP", label: "EXP", align: "right" },
];

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
          gridTemplateColumns: TEMPLATE,
          minWidth: MIN_WIDTH,
          height: rowH.colHead,
          alignItems: "center",
          background: c.surface,
          borderBottom: `1px solid ${c.lineStrong}`,
          position: "sticky",
          top: 0,
          zIndex: 2,
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
        {columns.map((col) => (
          <div
            key={col.key ?? "actions"}
            // A HEADING THAT SORTS IS A CONTROL, so it brightens on approach
            // like every other one. The unlabelled action column is not — it
            // holds CLR, which lights up on its own.
            className={col.key ? "dimhover" : undefined}
            onClick={col.key ? () => props.onSort(col.key as SortKey) : undefined}
            title={col.key ? `Sort by ${col.label}` : undefined}
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
              <>
                {col.label}
                {/* Neutral: which column is sorted is UI state, not a verdict. */}
                {col.key === sortKey && (
                  <span style={{ color: c.text }}>{sortDir === -1 ? " ▼" : " ▲"}</span>
                )}
              </>
            )}
          </div>
        ))}
      </div>

      <div style={{ minWidth: MIN_WIDTH }}>
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

  // Background is the COMMITTED state (in lineup / excluded); the focus edge is
  // the transient one. They no longer overwrite each other — a row that is both
  // in the lineup and being viewed is blue with a light edge, which is exactly
  // what it is. Previously focus replaced the lineup shading outright, so the
  // player you were reading about vanished from the lineup group while you read
  // about him.
  const background = inLineup
    ? c.lineupBg
    : isExcluded
      ? c.excludeBg
      : isSelected
        ? c.selectBg
        : undefined;
  const edge = isSelected ? c.focusEdge : inLineup ? c.blue : undefined;

  const p20pct = field.pct.P_TOP20[p.id];
  const p20col = rankColor(p20pct);
  const exp = exposure.get(p.id) ?? 0;

  return (
    <div
      // `.gridrow` carries the hover wash AND reveals the two action icons —
      // both in CSS rather than React state, because tracking the hovered row
      // here would re-render every row in the field on each mouse move. The
      // COMMITTED backgrounds below are inline, which beats the CSS rule, so
      // pointing at an in-lineup player never makes him stop looking like one.
      className="gridrow"
      data-player-id={p.id}
      onClick={() => onSelect(p.id)}
      style={{
        display: "grid",
        gridTemplateColumns: TEMPLATE,
        alignItems: "center",
        height: rowH.body,
        borderBottom: `1px solid ${c.lineSoft}`,
        fontFamily: font.data,
        fontSize: t.data,
        cursor: "pointer",
        background,
        boxShadow: edge ? `inset 2px 0 0 ${edge}` : undefined,
      }}
    >
      <div className="lx" style={{ paddingLeft: 8 }}>
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
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        }}
      >
        {p.PLAYER}
      </div>

      {/* Exception 2 — no colour. The ramp means "nearer the good end of the
          field", and there is no good end here: an $11,200 price tag is not
          better than a $6,400 one, it is the constraint you are spending. */}
      <div style={num(c.text2)}>{fmtSalary(p.SALARY)}</div>

      {/* EVERY column below is rankColor(), and that uniformity is the feature:
          one ramp, keyed on rank in this field, so a colour means the same thing
          in ODDS as it does in SG:F and the row reads across in one pass. Each
          of these used to argue its own case — LEV had a hand-tuned ±2 dead
          band, SG:F and SG:C coloured by sign, ODDS/CUT9M/OWGR ran a separate
          grey-only ramp with different breakpoints. Six private scales meant a
          green cell in one column and a green cell in the next were not claiming
          the same thing, which is precisely what made the grid hard to scan. */}
      <div style={{ ...num(p20col), fontWeight: weight.medium }}>{(p.P_TOP20 * 100).toFixed(1)}</div>

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
      {/* Exception 1 — amber. Over-exposure is a warning about YOUR build, not
          a measurement of the player, and a threshold breach is not a rank, so
          it is the one column that stays off the ramp. */}
      <div style={num(exp >= 60 ? c.amber : exp > 0 ? c.text2 : c.axis)}>
        {savedCount === 0 ? EM_DASH : `${exp.toFixed(0)}%`}
      </div>
    </div>
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
function LockIcon() {
  return (
    <svg viewBox="0 0 24 24" width={12} height={12} fill="none" stroke="currentColor"
      strokeWidth={2.3} strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <rect x="4" y="10.5" width="16" height="10" rx="2.2" />
      <path d="M8.2 10.5V7.2a3.8 3.8 0 0 1 7.6 0v3.3" />
    </svg>
  );
}

function BanIcon() {
  return (
    <svg viewBox="0 0 24 24" width={12} height={12} fill="none" stroke="currentColor"
      strokeWidth={2.3} strokeLinecap="round" aria-hidden="true">
      <circle cx="12" cy="12" r="8.6" />
      <path d="M6.1 6.1l11.8 11.8" />
    </svg>
  );
}

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
