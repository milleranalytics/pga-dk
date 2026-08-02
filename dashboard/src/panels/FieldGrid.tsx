import { useMemo } from "react";
import type { CSSProperties } from "react";
import { c, font } from "../tokens";
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

const TEMPLATE =
  "84px minmax(150px,1fr) 80px 112px 62px 54px 56px 60px 60px 54px 52px 62px";
const MIN_WIDTH = 906;

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
  { key: null, label: "＋ L X", align: "left" },
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
  onTogglePick: (id: string) => void;
  onToggleLock: (id: string) => void;
  onToggleExclude: (id: string) => void;
}

export default function FieldGrid(props: FieldGridProps) {
  const { field, query, sortKey, sortDir, exposure, savedCount } = props;

  const rows = useMemo(() => {
    const q = query.trim().toLowerCase();
    // Filter before sort, per the handoff.
    const filtered = q
      ? field.players.filter((p) => p.PLAYER.toLowerCase().includes(q))
      : field.players;

    const val = (p: Player): number | string => {
      if (sortKey === "PLAYER") return p.PLAYER.toLowerCase();
      if (sortKey === "EXP") return exposure.get(p.id) ?? 0;
      if (sortKey === "VAL") return p.VAL;
      return p[sortKey];
    };

    return [...filtered].sort((a, b) => {
      const av = val(a);
      const bv = val(b);
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
            key={col.label}
            onClick={col.key ? () => props.onSort(col.key as SortKey) : undefined}
            style={{
              textAlign: col.align,
              paddingLeft: col.align === "left" ? 10 : undefined,
              paddingRight: col.align === "right" ? 10 : undefined,
              cursor: col.key ? "pointer" : "default",
              userSelect: "none",
            }}
          >
            {col.label}
            {col.key === sortKey && (
              <span style={{ color: c.green }}>{sortDir === -1 ? " ▼" : " ▲"}</span>
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
  onTogglePick,
  onToggleLock,
  onToggleExclude,
}: FieldGridProps & { p: Player }) {
  const isSelected = selected === p.id;
  const inLineup = picks.includes(p.id);
  const isExcluded = !!excludes[p.id];

  // Selected wins over in-lineup wins over excluded.
  let background: string | undefined;
  let edge: string | undefined;
  if (isSelected) {
    background = c.selectBg;
    edge = c.blue;
  } else if (inLineup) {
    background = c.lineupBg;
    edge = c.green;
  } else if (isExcluded) {
    background = c.excludeBg;
  }

  const p20pct = field.pct.P_TOP20[p.id] ?? 0;
  const p20color = p20pct >= 0.8 ? c.green : p20pct >= 0.4 ? c.text2 : c.dim;
  const valPct = field.pct.VAL[p.id] ?? 0;
  const valColor = valPct >= 0.85 ? c.green : valPct <= 0.15 ? c.red : c.text2;
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
        <MiniBtn on={inLineup} onClick={() => onTogglePick(p.id)} size={12} label="＋" />
        <MiniBtn on={!!locks[p.id]} onClick={() => onToggleLock(p.id)} size={10} label="L" bold />
        <MiniBtn
          on={isExcluded}
          onClick={() => onToggleExclude(p.id)}
          size={10}
          label="X"
          bold
          danger
        />
      </div>

      <div
        style={{
          fontFamily: font.sans,
          fontSize: 13,
          fontWeight: 500,
          paddingLeft: 10,
          color: isExcluded ? c.dim : c.text,
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        }}
      >
        {p.PLAYER}
      </div>

      <div style={num(c.text2)}>{fmtSalary(p.SALARY)}</div>

      {/* Bar is scaled to the field's best P(top-20), not to 1.0 — otherwise
          every bar is short and the column carries no information. */}
      <div style={{ display: "flex", alignItems: "center", gap: 8, paddingRight: 10 }}>
        <div style={{ flex: 1, height: 5, borderRadius: 3, background: c.track }}>
          <div
            style={{
              height: "100%",
              borderRadius: 3,
              width: `${field.maxP20 > 0 ? (p.P_TOP20 / field.maxP20) * 100 : 0}%`,
              background: p20color,
            }}
          />
        </div>
        <div style={{ width: 34, textAlign: "right", color: p20color, fontWeight: 500 }}>
          {(p.P_TOP20 * 100).toFixed(1)}
        </div>
      </div>

      <div style={{ ...num(valColor), fontWeight: 500 }}>{p.VAL.toFixed(2)}</div>
      <div style={num(p.LEVERAGE >= 2 ? c.blue : p.LEVERAGE <= -2 ? c.amber : c.dim)}>
        {fmtDelta(p.LEVERAGE, 1)}
      </div>
      <div style={num(c.muted)}>{p.VEGAS_ODDS.toFixed(0)}</div>
      <div style={num(p.SG_FORM >= 0 ? c.greenSoft : c.redSoft)}>
        {fmtDelta(p.SG_FORM, 2)}
      </div>
      {/* Dim marks "no measurement at this course", not "neutral". Keyed on
          ch_window rather than the value being 0: the export rounds to 2dp, so
          a player at exactly field average also reads 0.00 and would otherwise
          be mislabelled as having no history. */}
      <div
        style={num(
          p.form?.ch_window === false
            ? c.dimmer
            : p.SG_CH_SHRUNK > 0
              ? c.greenSoft
              : p.SG_CH_SHRUNK < 0
                ? c.redSoft
                : c.text2,
        )}
      >
        {fmtDelta(p.SG_CH_SHRUNK, 2)}
      </div>
      <div style={num(c.muted)}>{p.CUT_PERCENTAGE.toFixed(0)}</div>
      <div style={num(c.dim)}>{p.OWGR_RANK.toFixed(0)}</div>
      <div style={num(exp >= 60 ? c.amber : exp > 0 ? c.text2 : c.axis)}>
        {savedCount === 0 ? EM_DASH : `${exp.toFixed(0)}%`}
      </div>
    </div>
  );
}

function num(color: string): CSSProperties {
  return { textAlign: "right", paddingRight: 10, color };
}

function MiniBtn({
  on,
  onClick,
  size,
  label,
  bold,
  danger,
}: {
  on: boolean;
  onClick: () => void;
  size: number;
  label: string;
  bold?: boolean;
  danger?: boolean;
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
        background: on ? (danger ? c.red : c.green) : "transparent",
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
