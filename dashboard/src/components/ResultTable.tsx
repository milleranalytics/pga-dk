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

export default function ResultTable({
  result,
  emptyText = "No rows.",
}: {
  result: QueryResult;
  emptyText?: string;
}) {
  const [filters, setFilters] = useState<Record<number, string>>({});

  const active = useMemo(
    () =>
      Object.entries(filters)
        .filter(([, v]) => v.trim() !== "")
        .map(([i, v]) => [Number(i), v.trim().toLowerCase()] as const),
    [filters],
  );

  const filtered = useMemo(() => {
    if (!active.length) return result.rows;
    return result.rows.filter((row) =>
      active.every(([i, v]) => String(row[i] ?? "").toLowerCase().includes(v)),
    );
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
                  padding: "7px 12px 3px",
                  whiteSpace: "nowrap",
                }}
              >
                {col}
              </th>
            ))}
          </tr>
          <tr>
            {result.columns.map((col, i) => (
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
                  value={filters[i] ?? ""}
                  onChange={(e) => setFilters((f) => ({ ...f, [i]: e.target.value }))}
                  placeholder="filter…"
                  style={{
                    width: "100%",
                    minWidth: 70,
                    background: c.bg,
                    border: `1px solid ${filters[i] ? c.green : c.lineStrong}`,
                    borderRadius: 3,
                    padding: "3px 6px",
                    fontSize: 10.5,
                    fontFamily: font.mono,
                    color: c.text,
                    outline: "none",
                  }}
                />
              </th>
            ))}
          </tr>
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
            <span style={{ color: c.green }}>{filtered.length.toLocaleString()}</span> of{" "}
            {result.rows.length.toLocaleString()} rows match ·{" "}
          </>
        )}
        {filtered.length > RENDER_LIMIT && (
          <span style={{ color: c.amber }}>
            rendering first {RENDER_LIMIT} — filter to narrow
          </span>
        )}
        {filtered.length <= RENDER_LIMIT && active.length === 0 && (
          <>{result.rows.length.toLocaleString()} rows</>
        )}
      </div>
    </div>
  );
}
