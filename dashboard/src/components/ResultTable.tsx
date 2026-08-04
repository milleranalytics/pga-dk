import { useMemo, useState } from "react";
import { c, font } from "../tokens";
import type { QueryResult } from "../db";

/**
 * Dense result grid with per-column filters, in the manner of DB Browser.
 *
 * Filtering happens in memory over the whole loaded result set, not over the
 * rendered rows, so typing in a filter searches everything that was fetched.
 * Only the first RENDER_LIMIT matches are put in the DOM — a 56k-row table
 * filters in a few milliseconds but would take seconds to lay out.
 */

const RENDER_LIMIT = 500;

type Cell = string | number | Uint8Array | null;

/**
 * Coerce a cell to a number for comparison filters.
 *
 * SQLite holds several columns in this database as text, so a naive Number()
 * would make comparison filters useless on exactly the columns most worth
 * comparing:
 *   ROUNDS:1..4      "-7", and "E" for even par
 *   OFFICIAL_MONEY   "$1,584,000.00"
 *   stats percentages "57.98%"
 * Returns null when the cell is not a number at all, and a null never satisfies
 * a comparison — so ">0" quietly excludes blanks rather than matching them.
 */
export function toNumber(cell: Cell): number | null {
  if (cell === null) return null;
  if (typeof cell === "number") return Number.isFinite(cell) ? cell : null;
  if (typeof cell !== "string") return null;
  const s = cell.trim();
  if (s === "") return null;
  if (s.toUpperCase() === "E") return 0; // golf: even par
  const n = Number(s.replace(/[$,%\s]/g, ""));
  return Number.isFinite(n) ? n : null;
}

const RANGE_RE = /^(-?\d*\.?\d+)\s*\.\.\s*(-?\d*\.?\d+)$/;
const CMP_RE = /^(>=|<=|<>|!=|>|<|=)\s*(-?\d*\.?\d+)$/;

/**
 * Turn a filter box into a predicate.
 *
 * Plain text stays a case-insensitive substring match — the default, and what
 * is wanted most of the time. An expression that is unambiguously numeric
 * (">20", "<=5", "10..20") switches that column to numeric comparison. There is
 * no per-column type declaration: the FILTER decides how it is read, so the
 * same box does both without a mode toggle.
 */
export function buildPredicate(raw: string): ((cell: Cell) => boolean) | null {
  const s = raw.trim();
  if (!s) return null;

  const range = s.match(RANGE_RE);
  if (range) {
    const a = parseFloat(range[1]);
    const b = parseFloat(range[2]);
    const lo = Math.min(a, b);
    const hi = Math.max(a, b);
    return (cell) => {
      const n = toNumber(cell);
      return n !== null && n >= lo && n <= hi;
    };
  }

  const cmp = s.match(CMP_RE);
  if (cmp) {
    const op = cmp[1];
    const v = parseFloat(cmp[2]);
    return (cell) => {
      const n = toNumber(cell);
      if (n === null) return false;
      switch (op) {
        case ">":
          return n > v;
        case "<":
          return n < v;
        case ">=":
          return n >= v;
        case "<=":
          return n <= v;
        case "=":
          return n === v;
        default:
          return n !== v; // <> and !=
      }
    };
  }

  const needle = s.toLowerCase();
  return (cell) => String(cell ?? "").toLowerCase().includes(needle);
}

const FILTER_HINT =
  "Text matches anywhere (case-insensitive).\nNumeric: >20  <5  >=10  <=3  =7  !=0  10..20\n\"E\" counts as 0; $ , % are ignored.";

export default function ResultTable({
  result,
  emptyText = "No rows.",
  columnFilters = true,
}: {
  result: QueryResult;
  emptyText?: string;
  /** Per-column filter row. Off where facet filters above already cover it. */
  columnFilters?: boolean;
}) {
  const [filters, setFilters] = useState<Record<number, string>>({});

  const active = useMemo(() => {
    if (!columnFilters) return [];
    return Object.entries(filters)
      .map(([i, v]) => [Number(i), buildPredicate(v)] as const)
      .filter((e): e is readonly [number, (cell: Cell) => boolean] => e[1] !== null);
  }, [filters, columnFilters]);

  const filtered = useMemo(() => {
    if (!active.length) return result.rows;
    return result.rows.filter((row) => active.every(([i, pred]) => pred(row[i] ?? null)));
  }, [result.rows, active]);

  const shown = filtered.slice(0, RENDER_LIMIT);

  if (result.rows.length === 0) {
    return <div style={{ padding: 16, color: c.dim, fontSize: 12 }}>{emptyText}</div>;
  }

  return (
    <div>
      <table
        style={{
          borderCollapse: "collapse",
          fontFamily: font.mono,
          fontSize: 11.5,
          width: "max-content",
          minWidth: "100%",
        }}
      >
        <thead>
          <tr>
            {result.columns.map((col) => (
              <th
                key={col}
                style={{
                  position: "sticky",
                  top: 0,
                  zIndex: 2,
                  background: c.surface,
                  color: c.muted,
                  fontSize: 10,
                  letterSpacing: "0.06em",
                  fontWeight: 400,
                  textAlign: "left",
                  padding: columnFilters ? "7px 12px 3px" : "7px 12px",
                  borderBottom: columnFilters ? undefined : `1px solid ${c.lineStrong}`,
                  whiteSpace: "nowrap",
                }}
              >
                {col}
              </th>
            ))}
          </tr>
          {columnFilters && (
            <tr>
              {result.columns.map((col, i) => {
                const raw = filters[i] ?? "";
                const numeric = RANGE_RE.test(raw.trim()) || CMP_RE.test(raw.trim());
                return (
                  <th
                    key={col}
                    style={{
                      position: "sticky",
                      top: 26,
                      zIndex: 2,
                      background: c.surface,
                      borderBottom: `1px solid ${c.lineStrong}`,
                      padding: "0 8px 6px",
                    }}
                  >
                    <input
                      value={raw}
                      onChange={(e) => setFilters((f) => ({ ...f, [i]: e.target.value }))}
                      placeholder="filter / >20"
                      title={FILTER_HINT}
                      style={{
                        width: "100%",
                        minWidth: 74,
                        background: c.bg,
                        // An active filter borders blue (rule 2: a control you
                        // are driving). Numeric-comparison mode additionally
                        // turns the TEXT blue — the mode still has to be
                        // visible, but it used to be marked in green, which
                        // implied the filter was somehow good.
                        border: `1px solid ${raw ? c.blue : c.lineStrong}`,
                        borderRadius: 3,
                        padding: "3px 6px",
                        fontSize: 10.5,
                        fontFamily: font.mono,
                        color: numeric ? c.blue : c.text,
                        outline: "none",
                      }}
                    />
                  </th>
                );
              })}
            </tr>
          )}
        </thead>
        <tbody>
          {shown.map((row, i) => (
            <tr key={i}>
              {row.map((cell, j) => (
                <td
                  key={j}
                  style={{
                    borderBottom: `1px solid ${c.lineSoft}`,
                    padding: "5px 12px",
                    whiteSpace: "nowrap",
                    textAlign: typeof cell === "number" ? "right" : "left",
                    color: cell === null ? c.dimmer : typeof cell === "number" ? c.text2 : c.text,
                  }}
                >
                  {cell === null ? "—" : String(cell)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>

      <div
        style={{
          padding: "8px 12px",
          fontFamily: font.mono,
          fontSize: 10.5,
          color: c.dim,
          borderTop: `1px solid ${c.lineSoft}`,
        }}
      >
        {active.length > 0 && (
          <>
            <span style={{ color: c.text }}>{filtered.length.toLocaleString()}</span> of{" "}
            {result.rows.length.toLocaleString()} rows match ·{" "}
          </>
        )}
        {filtered.length > RENDER_LIMIT ? (
          <span style={{ color: c.amber }}>
            rendering first {RENDER_LIMIT.toLocaleString()}
            {columnFilters ? " — filter to narrow" : ""}
          </span>
        ) : (
          active.length === 0 && <>{result.rows.length.toLocaleString()} rows</>
        )}
        {columnFilters && (
          <span style={{ color: c.dimmer }}>
            {"  ·  "}filters accept text or {">20"} {"<=5"} {"10..20"}
          </span>
        )}
      </div>
    </div>
  );
}
