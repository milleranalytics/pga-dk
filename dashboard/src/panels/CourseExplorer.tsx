import { useMemo, useState } from "react";
import { c, font, dirColor } from "../tokens";
import type { CourseTable } from "../types";
import type { Field } from "../enrich";
import { fmtSigned } from "../format";

/**
 * Course Explorer — horses for courses, measured in STROKES rather than finish
 * positions, which mix in field strength and luck.
 *
 * Two SG columns on purpose:
 *  - SG (model) is SG_CH_SHRUNK itself, the exact feature the model consumes
 *    (7-year window, shrunk toward field average by 2 pseudo-rounds). This
 *    column reconciles with the SG:C column in the field grid.
 *  - SG (raw) is the plain unshrunk mean. The gap between the two IS the
 *    sample-size warning: a big gap means thin history.
 *
 * Only this week's course is available — see the note on Slate["course"].
 */

const T = "38px minmax(150px,1fr) 66px 66px 46px 46px 78px 52px 46px 62px";

type SortKey =
  | "sg_model"
  | "sg_raw"
  | "rounds"
  | "events"
  | "avg_finish_pct"
  | "cut_pct"
  | "best"
  | "last_played"
  | "player";

export default function CourseExplorer({
  course,
  field,
  onSelect,
}: {
  course: CourseTable;
  field: Field;
  onSelect: (id: string) => void;
}) {
  const [query, setQuery] = useState("");
  const [minRounds, setMinRounds] = useState(8);
  const [fieldOnly, setFieldOnly] = useState(true);
  const [sortKey, setSortKey] = useState<SortKey>("sg_model");
  const [sortDir, setSortDir] = useState<1 | -1>(-1);

  const rows = useMemo(() => {
    const q = query.trim().toLowerCase();
    let r = course.players.filter((p) => p.rounds >= minRounds);
    if (fieldOnly) r = r.filter((p) => field.byId.has(p.player));
    if (q) r = r.filter((p) => p.player.toLowerCase().includes(q));
    return [...r].sort((a, b) => {
      if (sortKey === "player") return a.player.localeCompare(b.player) * sortDir;
      const av = a[sortKey] ?? -Infinity;
      const bv = b[sortKey] ?? -Infinity;
      return ((av as number) - (bv as number)) * sortDir;
    });
  }, [course.players, query, minRounds, fieldOnly, sortKey, sortDir, field.byId]);

  const sort = (k: SortKey) => {
    if (k === sortKey) setSortDir((d) => (d === 1 ? -1 : 1));
    else {
      setSortKey(k);
      // Lower is better for these two; everything else is biggest-first.
      setSortDir(k === "avg_finish_pct" || k === "best" || k === "player" ? 1 : -1);
    }
  };

  if (!course.players.length) {
    return (
      <div style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center", color: c.dim, fontSize: 12.5 }}>
        No history at {course.course} in the database.
      </div>
    );
  }

  const headers: [SortKey | null, string][] = [
    [null, "#"],
    ["player", "PLAYER"],
    ["sg_model", "SG MODEL"],
    ["sg_raw", "SG RAW"],
    ["rounds", "RDS"],
    ["events", "EV"],
    ["avg_finish_pct", "AVG FIN PCT"],
    ["cut_pct", "CUT%"],
    ["best", "BEST"],
    ["last_played", "LAST"],
  ];

  return (
    <div style={{ flex: 1, display: "flex", flexDirection: "column", minHeight: 0 }}>
      <div
        style={{
          padding: "12px 16px",
          borderBottom: `1px solid ${c.line}`,
          display: "flex",
          alignItems: "center",
          gap: 14,
          flexWrap: "wrap",
        }}
      >
        <div>
          <div style={{ fontSize: 15, fontWeight: 600 }}>{course.course}</div>
          <div style={{ fontFamily: font.mono, fontSize: 10.5, color: c.dim, marginTop: 2 }}>
            {course.events} events · {course.first_year}–{course.last_year}
          </div>
        </div>
        <input
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Filter player…"
          style={inputStyle}
        />
        <label style={ctlLabel}>
          MIN RDS
          <input
            type="range"
            min={4}
            max={20}
            value={minRounds}
            onChange={(e) => setMinRounds(Number(e.target.value))}
            style={{ width: 90, accentColor: c.blue }}
          />
          <span style={{ color: c.text2, width: 16 }}>{minRounds}</span>
        </label>
        <label style={{ ...ctlLabel, cursor: "pointer" }}>
          <input
            type="checkbox"
            checked={fieldOnly}
            onChange={(e) => setFieldOnly(e.target.checked)}
            style={{ accentColor: c.blue }}
          />
          THIS WEEK&apos;S FIELD ONLY
        </label>
        <span style={{ fontFamily: font.mono, fontSize: 10.5, color: c.dim, marginLeft: "auto" }}>
          {rows.length} players
        </span>
      </div>

      <div style={{ flex: 1, overflow: "auto", minHeight: 0 }}>
        <div
          style={{
            display: "grid",
            gridTemplateColumns: T,
            minWidth: 780,
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
          {headers.map(([key, label], i) => (
            <div
              key={label}
              onClick={key ? () => sort(key) : undefined}
              style={{
                textAlign: i <= 1 ? "left" : "right",
                paddingLeft: i <= 1 ? 10 : undefined,
                paddingRight: i > 1 ? 10 : undefined,
                cursor: key ? "pointer" : "default",
                userSelect: "none",
              }}
            >
              {label}
              {key === sortKey && <span style={{ color: c.text }}>{sortDir === -1 ? " ▼" : " ▲"}</span>}
            </div>
          ))}
        </div>

        <div style={{ minWidth: 780 }}>
          {rows.map((p, i) => {
            const inField = field.byId.has(p.player);
            return (
              <div
                key={p.player}
                onClick={() => inField && onSelect(p.player)}
                style={{
                  display: "grid",
                  gridTemplateColumns: T,
                  alignItems: "center",
                  height: 30,
                  borderBottom: `1px solid ${c.lineSoft}`,
                  fontFamily: font.mono,
                  fontSize: 11.5,
                  cursor: inField ? "pointer" : "default",
                }}
              >
                <div style={{ paddingLeft: 10, color: c.dim }}>{i + 1}</div>
                <div
                  style={{
                    fontFamily: font.sans,
                    fontSize: 12.5,
                    paddingLeft: 10,
                    color: inField ? c.text : c.dim,
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                    whiteSpace: "nowrap",
                  }}
                >
                  {p.player}
                </div>
                <div style={{ ...cell, color: dirColor(p.sg_model ?? 0), fontWeight: 500 }}>
                  {p.sg_model === null ? "—" : fmtSigned(p.sg_model, 2)}
                </div>
                <div style={{ ...cell, color: dirColor(p.sg_raw) }}>
                  {fmtSigned(p.sg_raw, 2)}
                </div>
                <div style={{ ...cell, color: c.muted }}>{p.rounds}</div>
                <div style={{ ...cell, color: c.muted }}>{p.events}</div>
                {/* 0 = won, 1 = last; the cut bucket sits near 0.58 */}
                <div style={{ ...cell, color: c.text2 }}>{p.avg_finish_pct.toFixed(3)}</div>
                <div style={{ ...cell, color: c.muted }}>{p.cut_pct}</div>
                <div style={{ ...cell, color: p.best <= 10 ? c.green : c.text2 }}>{p.best}</div>
                <div style={{ ...cell, color: c.dim }}>{p.last_played}</div>
              </div>
            );
          })}
        </div>
      </div>

      <div
        style={{
          padding: "8px 16px",
          borderTop: `1px solid ${c.line}`,
          fontSize: 11,
          color: c.dim,
          lineHeight: 1.5,
        }}
      >
        <b style={{ color: c.muted }}>SG MODEL</b> is SG_CH_SHRUNK — the exact feature the model
        uses (7-year window, shrunk toward field average for small samples), so it matches the
        SG:C column. <b style={{ color: c.muted }}>SG RAW</b> is the plain unshrunk average; a
        large gap between the two means thin history. <b style={{ color: c.muted }}>AVG FIN PCT</b>{" "}
        is finish percentile — 0 = won, 1 = last, cut bucket ≈ 0.58.
      </div>
    </div>
  );
}

const cell = { textAlign: "right", paddingRight: 10 } as const;

const inputStyle: React.CSSProperties = {
  width: 160,
  background: c.surfaceAlt,
  border: `1px solid ${c.lineStrong}`,
  borderRadius: 4,
  padding: "6px 10px",
  fontSize: 12,
  color: c.text,
  outline: "none",
  fontFamily: font.sans,
};

const ctlLabel: React.CSSProperties = {
  display: "flex",
  alignItems: "center",
  gap: 7,
  fontFamily: font.mono,
  fontSize: 9.5,
  letterSpacing: "0.1em",
  color: c.dim,
};
