import { useEffect, useMemo, useRef, useState } from "react";
import type { CSSProperties } from "react";
import { c, font, rankColor, nameColor } from "../tokens";
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
const TEMPLATE =
  "60px minmax(150px,1fr) 80px 62px 62px 54px 56px 60px 60px 54px 52px 62px";
const MIN_WIDTH = 832;

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
          height: 30,
          alignItems: "center",
          background: c.surface,
          borderBottom: `1px solid ${c.lineStrong}`,
          position: "sticky",
          top: 0,
          zIndex: 2,
          fontFamily: font.mono,
          fontSize: 10,
          letterSpacing: "0.09em",
          color: c.muted,
        }}
      >
        {columns.map((col) => (
          <div
            key={col.key ?? "actions"}
            onClick={col.key ? () => props.onSort(col.key as SortKey) : undefined}
            style={{
              textAlign: col.align,
              paddingLeft: col.align === "left" ? 10 : undefined,
              paddingRight: col.align === "right" ? 10 : undefined,
              cursor: col.key ? "pointer" : "default",
              userSelect: "none",
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
      onClick={() => onSelect(p.id)}
      style={{
        display: "grid",
        gridTemplateColumns: TEMPLATE,
        alignItems: "center",
        height: 34,
        borderBottom: `1px solid ${c.lineSoft}`,
        fontFamily: font.mono,
        fontSize: 12,
        cursor: "pointer",
        background,
        boxShadow: edge ? `inset 2px 0 0 ${edge}` : undefined,
      }}
    >
      <div style={{ display: "flex", gap: 3, paddingLeft: 8 }}>
        <MiniBtn
          on={!!locks[p.id]}
          onClick={() => onToggleLock(p.id)}
          size={10}
          label="L"
          bold
        />
        <MiniBtn
          on={isExcluded}
          onClick={() => onToggleExclude(p.id)}
          size={10}
          label="X"
          bold
          tone="bad"
        />
      </div>

      <div
        style={{
          fontFamily: font.sans,
          fontSize: 13,
          // Names are the "title" in the Windows-Settings pairing the grey ramp
          // is modelled on: bold, with every number a step below. Which step is
          // nameColor's business — the selected row's name is the brightest in
          // the column, a third cue alongside the wash and the edge.
          fontWeight: 600,
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
      <div style={{ ...num(p20col), fontWeight: 500 }}>{(p.P_TOP20 * 100).toFixed(1)}</div>

      <div style={{ ...num(rankColor(field.pct.VAL[p.id])), fontWeight: 500 }}>
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
        fontFamily: font.mono,
        fontSize: 9,
        fontWeight: 600,
        letterSpacing: "0.08em",
        // Amber is rule 3 — a warning about your own state, not a data verdict.
        color: armed ? c.amber : c.dim,
        cursor: "pointer",
        lineHeight: 1,
      }}
    >
      {armed ? `CLR ${total}?` : "CLR"}
    </button>
  );
}

function MiniBtn({
  on,
  onClick,
  size,
  label,
  bold,
  tone = "good",
}: {
  on: boolean;
  onClick: () => void;
  size: number;
  label: string;
  bold?: boolean;
  /** What the active state MEANS, per the colour rules: `lineup` is membership
   *  (blue), `good` is a favourable verdict (green), `bad` an exclusion (red). */
  tone?: "lineup" | "good" | "bad";
}) {
  return (
    <button
      onClick={(e) => {
        // Must not also select the row.
        e.stopPropagation();
        onClick();
      }}
      style={{
        width: 20,
        height: 20,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        border: `1px solid ${c.lineStrong}`,
        borderRadius: 3,
        background: on ? (tone === "bad" ? c.red : tone === "lineup" ? c.blue : c.green) : "transparent",
        color: on ? "#0b0d10" : c.dim,
        fontSize: size,
        fontWeight: bold ? 600 : 400,
        fontFamily: font.mono,
        padding: 0,
        cursor: "pointer",
        lineHeight: 1,
      }}
    >
      {label}
    </button>
  );
}
