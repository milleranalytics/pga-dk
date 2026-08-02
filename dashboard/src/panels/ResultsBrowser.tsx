import { useCallback, useEffect, useState } from "react";
import type { Database } from "sql.js";
import { c, font } from "../tokens";
import { loadDatabase, runQuery, listTables, tableColumns } from "../db";
import type { QueryResult } from "../db";

/**
 * Results Browser — free-form exploration of the FULL database.
 *
 * The whole reason the app loads golf.db rather than a pre-exported slice: the
 * point is to be able to ask questions nobody anticipated. So this is a SQL box
 * over the real file, with the schema listed beside it and a set of starting
 * queries, rather than a fixed report with three filters.
 */

const PRESETS: { label: string; sql: string }[] = [
  {
    label: "This week's field, career results",
    sql: `SELECT PLAYER, COUNT(*) AS events, ROUND(AVG(FINAL_POS),1) AS avg_finish,
       MIN(FINAL_POS) AS best,
       ROUND(100.0*AVG(CASE WHEN POS NOT IN ('CUT','W/D') THEN 1 ELSE 0 END),0) AS cut_pct
FROM tournaments
GROUP BY PLAYER
HAVING events >= 20
ORDER BY avg_finish
LIMIT 100;`,
  },
  {
    label: "Course leaderboard — all time",
    sql: `SELECT COURSE, PLAYER, COUNT(*) AS events, ROUND(AVG(FINAL_POS),1) AS avg_finish
FROM tournaments
WHERE COURSE = 'Detroit Golf Club'
GROUP BY COURSE, PLAYER
HAVING events >= 2
ORDER BY avg_finish
LIMIT 100;`,
  },
  {
    label: "Prediction log vs results",
    sql: `SELECT p.ENDING_DATE, p.TOURNAMENT, p.PLAYER,
       ROUND(p.P_TOP20,3) AS pred, t.POS, t.FINAL_POS
FROM predictions p
LEFT JOIN tournaments t
  ON t.TOURNAMENT = p.TOURNAMENT
 AND t.ENDING_DATE = p.ENDING_DATE
 AND t.PLAYER = p.PLAYER
ORDER BY p.ENDING_DATE DESC, pred DESC
LIMIT 200;`,
  },
  {
    label: "Season stats leaders",
    sql: `SELECT PLAYER, SGTTG, SGOTT, SGAPR, SGATG, SGP, OWGR_RANK
FROM stats
WHERE SEASON = (SELECT MAX(SEASON) FROM stats) AND SGTTG IS NOT NULL
ORDER BY SGTTG DESC
LIMIT 100;`,
  },
  {
    label: "Odds coverage by season",
    sql: `SELECT SEASON, COUNT(DISTINCT TOURNAMENT) AS events, COUNT(*) AS odds_rows
FROM odds
GROUP BY SEASON
ORDER BY SEASON DESC;`,
  },
];

export default function ResultsBrowser() {
  const [db, setDb] = useState<Database | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [tables, setTables] = useState<{ name: string; rows: number }[]>([]);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [cols, setCols] = useState<string[]>([]);

  const [sql, setSql] = useState(PRESETS[0].sql);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [queryError, setQueryError] = useState<string | null>(null);
  const [elapsed, setElapsed] = useState<number | null>(null);

  useEffect(() => {
    let cancelled = false;
    loadDatabase()
      .then((d) => {
        if (cancelled) return;
        setDb(d);
        setTables(listTables(d));
      })
      .catch((e: Error) => !cancelled && setLoadError(e.message));
    return () => {
      cancelled = true;
    };
  }, []);

  const run = useCallback(
    (text: string) => {
      if (!db) return;
      const t0 = performance.now();
      try {
        setResult(runQuery(db, text));
        setQueryError(null);
      } catch (e) {
        setQueryError((e as Error).message);
        setResult(null);
      }
      setElapsed(performance.now() - t0);
    },
    [db],
  );

  // Run the opening preset once the DB is ready, so the tab is never an empty box.
  useEffect(() => {
    if (db && result === null && queryError === null) run(sql);
  }, [db, run, sql, result, queryError]);

  if (loadError) {
    return (
      <Centered>
        <div style={{ color: c.red, fontFamily: font.mono, fontSize: 12, whiteSpace: "pre-wrap" }}>
          {loadError}
        </div>
      </Centered>
    );
  }
  if (!db) return <Centered>Loading golf.db…</Centered>;

  return (
    <div style={{ flex: 1, display: "flex", minHeight: 0 }}>
      {/* schema sidebar */}
      <div
        style={{
          flex: "none",
          width: 210,
          borderRight: `1px solid ${c.line}`,
          overflowY: "auto",
          padding: "12px 0",
        }}
      >
        <div style={{ padding: "0 14px 8px", fontFamily: font.mono, fontSize: 10, fontWeight: 600, letterSpacing: "0.14em", color: c.muted }}>
          SCHEMA
        </div>
        {tables.map((t) => (
          <div key={t.name}>
            <div
              onClick={() => {
                const next = expanded === t.name ? null : t.name;
                setExpanded(next);
                setCols(next ? tableColumns(db, next) : []);
              }}
              style={{
                padding: "5px 14px",
                display: "flex",
                justifyContent: "space-between",
                cursor: "pointer",
                fontFamily: font.mono,
                fontSize: 11.5,
                color: expanded === t.name ? c.green : c.text2,
                background: expanded === t.name ? c.selectBg : undefined,
              }}
            >
              <span>{t.name}</span>
              <span style={{ color: c.dim }}>{t.rows.toLocaleString()}</span>
            </div>
            {expanded === t.name &&
              cols.map((col) => (
                <div
                  key={col}
                  onClick={() => setSql((s) => s + col)}
                  style={{
                    padding: "3px 14px 3px 24px",
                    fontFamily: font.mono,
                    fontSize: 10.5,
                    color: c.dim,
                    cursor: "pointer",
                  }}
                >
                  {col}
                </div>
              ))}
          </div>
        ))}
      </div>

      <div style={{ flex: 1, display: "flex", flexDirection: "column", minWidth: 0 }}>
        <div style={{ padding: "12px 16px 8px", borderBottom: `1px solid ${c.line}` }}>
          <div style={{ display: "flex", gap: 6, flexWrap: "wrap", marginBottom: 8 }}>
            {PRESETS.map((p) => (
              <button
                key={p.label}
                onClick={() => {
                  setSql(p.sql);
                  run(p.sql);
                }}
                style={{
                  border: `1px solid ${c.lineStrong}`,
                  background: "transparent",
                  color: c.text2,
                  fontSize: 11,
                  padding: "5px 9px",
                  borderRadius: 4,
                  cursor: "pointer",
                  fontFamily: font.sans,
                }}
              >
                {p.label}
              </button>
            ))}
          </div>
          <textarea
            value={sql}
            onChange={(e) => setSql(e.target.value)}
            onKeyDown={(e) => {
              // Ctrl/Cmd+Enter runs — Enter alone must stay a newline in SQL.
              if ((e.ctrlKey || e.metaKey) && e.key === "Enter") run(sql);
            }}
            spellCheck={false}
            style={{
              width: "100%",
              height: 132,
              background: c.surfaceAlt,
              color: c.text,
              border: `1px solid ${c.lineStrong}`,
              borderRadius: 4,
              padding: 10,
              fontFamily: font.mono,
              fontSize: 12,
              lineHeight: 1.5,
              outline: "none",
              resize: "vertical",
            }}
          />
          <div style={{ display: "flex", alignItems: "center", gap: 10, marginTop: 8 }}>
            <button
              onClick={() => run(sql)}
              style={{
                background: c.green,
                color: "#0b0d10",
                border: "none",
                borderRadius: 4,
                padding: "7px 16px",
                fontSize: 12,
                fontWeight: 600,
                cursor: "pointer",
                fontFamily: font.sans,
              }}
            >
              Run
            </button>
            <span style={{ fontFamily: font.mono, fontSize: 10.5, color: c.dim }}>
              ⌘/Ctrl + Enter
            </span>
            {result && (
              <span style={{ fontFamily: font.mono, fontSize: 11, color: c.muted }}>
                {result.rows.length.toLocaleString()} row
                {result.rows.length === 1 ? "" : "s"}
                {result.truncated && <span style={{ color: c.amber }}> (capped at 2,000)</span>}
                {elapsed !== null && ` · ${elapsed.toFixed(0)} ms`}
              </span>
            )}
          </div>
        </div>

        <div style={{ flex: 1, overflow: "auto", minHeight: 0 }}>
          {queryError ? (
            <div style={{ padding: 16, color: c.red, fontFamily: font.mono, fontSize: 12 }}>
              {queryError}
            </div>
          ) : result ? (
            <ResultTable result={result} />
          ) : null}
        </div>
      </div>
    </div>
  );
}

function ResultTable({ result }: { result: QueryResult }) {
  if (result.rows.length === 0) {
    return <div style={{ padding: 16, color: c.dim, fontSize: 12 }}>No rows.</div>;
  }
  return (
    <table style={{ borderCollapse: "collapse", fontFamily: font.mono, fontSize: 11.5, width: "max-content", minWidth: "100%" }}>
      <thead>
        <tr>
          {result.columns.map((col) => (
            <th
              key={col}
              style={{
                position: "sticky",
                top: 0,
                background: c.surface,
                borderBottom: `1px solid ${c.lineStrong}`,
                color: c.muted,
                fontSize: 10,
                letterSpacing: "0.06em",
                fontWeight: 400,
                textAlign: "left",
                padding: "7px 12px",
                whiteSpace: "nowrap",
              }}
            >
              {col}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {result.rows.map((row, i) => (
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
  );
}

function Centered({ children }: { children: React.ReactNode }) {
  return (
    <div style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center", padding: 40, color: c.dim, fontSize: 12.5 }}>
      {children}
    </div>
  );
}
