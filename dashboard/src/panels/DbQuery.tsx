import { useCallback, useEffect, useState } from "react";
import type { Database } from "sql.js";
import { c, font } from "../tokens";
import { loadDatabase, runQuery, listTables, tableColumns, BROWSE_LIMIT } from "../db";
import type { QueryResult } from "../db";
import { useSavedQueries } from "../queries";
import ResultTable from "../components/ResultTable";

/**
 * DB Query — raw SQL against the full database.
 *
 * The counterpart to the Results tab: that one answers the questions asked
 * often, this one answers the questions nobody anticipated. Both read the same
 * data/golf.db via sql.js.
 *
 * Clicking a table name in the schema sidebar loads the WHOLE table (up to
 * BROWSE_LIMIT) rather than a page of it, so the per-column filters in
 * ResultTable search the real contents. That is the debugging workflow: load
 * `tournaments`, type a name into the PLAYER filter, see every row.
 */

export default function DbQuery() {
  const [db, setDb] = useState<Database | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [tables, setTables] = useState<{ name: string; rows: number }[]>([]);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [cols, setCols] = useState<string[]>([]);

  const { queries, save, remove, resetToBuiltins } = useSavedQueries();
  const [sql, setSql] = useState(queries[0]?.sql ?? "SELECT * FROM tournaments LIMIT 100;");
  const [name, setName] = useState(queries[0]?.name ?? "");
  const [result, setResult] = useState<QueryResult | null>(null);
  const [queryError, setQueryError] = useState<string | null>(null);
  const [elapsed, setElapsed] = useState<number | null>(null);
  const [ran, setRan] = useState(false);

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
    (text: string, limit?: number) => {
      if (!db) return;
      const t0 = performance.now();
      try {
        setResult(runQuery(db, text, [], limit));
        setQueryError(null);
      } catch (e) {
        setQueryError((e as Error).message);
        setResult(null);
      }
      setElapsed(performance.now() - t0);
      setRan(true);
    },
    [db],
  );

  // Run the opening query once, so the tab is never an empty box.
  useEffect(() => {
    if (db && !ran) run(sql);
  }, [db, ran, run, sql]);

  const browseTable = useCallback(
    (table: string) => {
      const q = `SELECT * FROM "${table}";`;
      setSql(q);
      setName("");
      run(q, BROWSE_LIMIT);
    },
    [run],
  );

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
          width: 220,
          borderRight: `1px solid ${c.line}`,
          overflowY: "auto",
          padding: "12px 0",
        }}
      >
        <div
          style={{
            padding: "0 14px 8px",
            fontFamily: font.mono,
            fontSize: 10,
            fontWeight: 600,
            letterSpacing: "0.14em",
            color: c.muted,
          }}
        >
          SCHEMA
        </div>
        <div style={{ padding: "0 14px 8px", fontSize: 10.5, color: c.dim, lineHeight: 1.4 }}>
          Click a table to load all of it, then filter columns in the header.
        </div>
        {tables.map((t) => (
          <div key={t.name}>
            <div
              style={{
                display: "flex",
                alignItems: "center",
                gap: 6,
                padding: "5px 14px",
                fontFamily: font.mono,
                fontSize: 11.5,
                background: expanded === t.name ? c.selectBg : undefined,
              }}
            >
              <span
                onClick={() => {
                  const next = expanded === t.name ? null : t.name;
                  setExpanded(next);
                  setCols(next ? tableColumns(db, next) : []);
                }}
                style={{ cursor: "pointer", color: c.dim, width: 10 }}
                title="Show columns"
              >
                {expanded === t.name ? "▾" : "▸"}
              </span>
              <span
                onClick={() => browseTable(t.name)}
                style={{ cursor: "pointer", flex: 1, color: c.text2 }}
                title={`Load all ${t.rows.toLocaleString()} rows`}
              >
                {t.name}
              </span>
              <span style={{ color: c.dim }}>{t.rows.toLocaleString()}</span>
            </div>
            {expanded === t.name &&
              cols.map((col) => (
                <div
                  key={col}
                  onClick={() => setSql((s) => s + col)}
                  style={{
                    padding: "3px 14px 3px 30px",
                    fontFamily: font.mono,
                    fontSize: 10.5,
                    color: c.dim,
                    cursor: "pointer",
                  }}
                  title="Insert into the query"
                >
                  {col}
                </div>
              ))}
          </div>
        ))}
      </div>

      <div style={{ flex: 1, display: "flex", flexDirection: "column", minWidth: 0 }}>
        <div style={{ padding: "10px 16px 8px", borderBottom: `1px solid ${c.line}` }}>
          <div
            style={{
              display: "flex",
              gap: 6,
              flexWrap: "wrap",
              marginBottom: 8,
              alignItems: "center",
            }}
          >
            {queries.map((q) => (
              <span
                key={q.name}
                style={{
                  display: "inline-flex",
                  alignItems: "center",
                  border: `1px solid ${name === q.name ? c.blue : c.lineStrong}`,
                  borderRadius: 4,
                  overflow: "hidden",
                }}
              >
                <button
                  onClick={() => {
                    setSql(q.sql);
                    setName(q.name);
                    run(q.sql);
                  }}
                  style={{
                    border: "none",
                    background: "transparent",
                    color: name === q.name ? c.blue : c.text2,
                    fontSize: 11,
                    padding: "5px 8px",
                    cursor: "pointer",
                    fontFamily: font.sans,
                  }}
                >
                  {q.name}
                </button>
                <button
                  onClick={() => remove(q.name)}
                  title="Delete this saved query"
                  style={{
                    border: "none",
                    borderLeft: `1px solid ${c.lineStrong}`,
                    background: "transparent",
                    color: c.dim,
                    fontSize: 10,
                    padding: "5px 6px",
                    cursor: "pointer",
                  }}
                >
                  ✕
                </button>
              </span>
            ))}
            {queries.length === 0 && (
              <button onClick={resetToBuiltins} style={ghostBtn}>
                restore built-in queries
              </button>
            )}
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
              // ~10 lines at 12px/1.5 — long joins fit without scrolling.
              height: 182,
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

          <div
            style={{ display: "flex", alignItems: "center", gap: 8, marginTop: 8, flexWrap: "wrap" }}
          >
            <button onClick={() => run(sql)} style={primaryBtn}>
              Run
            </button>
            <span style={{ fontFamily: font.mono, fontSize: 10.5, color: c.dim }}>⌘/Ctrl+Enter</span>

            <span style={{ width: 12 }} />

            <input
              value={name}
              onChange={(e) => setName(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") save(name, sql);
              }}
              placeholder="name this query…"
              style={{
                width: 180,
                background: c.surfaceAlt,
                border: `1px solid ${c.lineStrong}`,
                borderRadius: 4,
                padding: "6px 9px",
                fontSize: 11.5,
                color: c.text,
                outline: "none",
                fontFamily: font.sans,
              }}
            />
            <button
              onClick={() => save(name, sql)}
              disabled={!name.trim()}
              title="Saving under an existing name overwrites it"
              style={{
                ...ghostBtn,
                opacity: name.trim() ? 1 : 0.4,
                cursor: name.trim() ? "pointer" : "default",
              }}
            >
              Save
            </button>

            {result && (
              <span
                style={{ fontFamily: font.mono, fontSize: 11, color: c.muted, marginLeft: "auto" }}
              >
                {result.rows.length.toLocaleString()} row{result.rows.length === 1 ? "" : "s"}
                {result.truncated && <span style={{ color: c.amber }}> (capped)</span>}
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

const primaryBtn: React.CSSProperties = {
  background: c.blue,
  color: "#0b0d10",
  border: "none",
  borderRadius: 4,
  padding: "7px 16px",
  fontSize: 12,
  fontWeight: 600,
  cursor: "pointer",
  fontFamily: font.sans,
};

const ghostBtn: React.CSSProperties = {
  border: `1px solid ${c.lineStrong}`,
  background: "transparent",
  color: c.text2,
  fontSize: 11.5,
  padding: "6px 12px",
  borderRadius: 4,
  cursor: "pointer",
  fontFamily: font.sans,
};

function Centered({ children }: { children: React.ReactNode }) {
  return (
    <div
      style={{
        flex: 1,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        padding: 40,
        color: c.dim,
        fontSize: 12.5,
      }}
    >
      {children}
    </div>
  );
}
