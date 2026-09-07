import { useCallback, useEffect, useState } from "react";
import type { Database } from "sql.js";
import { c, font, type as ty, weight } from "../tokens";
import { Caret, Cross } from "../components/icons";
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
        <div style={{ color: c.red, fontFamily: font.code, fontSize: ty.data, whiteSpace: "pre-wrap" }}>
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
            fontFamily: font.data,
            fontSize: ty.colhead,
            fontWeight: weight.semi,
            letterSpacing: "0.14em",
            color: c.muted,
          }}
        >
          SCHEMA
        </div>
        <div style={{ padding: "0 14px 8px", fontSize: ty.chip, color: c.dim, lineHeight: 1.4 }}>
          Click a table to load all of it, then filter columns in the header.
        </div>
        {tables.map((t) => (
          <div key={t.name}>
            <div
              // `.clickrow` supplies the hover wash; the EXPANDED table's wash
              // is set inline below and therefore survives it, so the table you
              // have opened stays marked while you point at another.
              className="clickrow"
              style={{
                display: "flex",
                alignItems: "center",
                gap: 6,
                padding: "5px 14px",
                // A TABLE NAME IS AN IDENTIFIER, not a label — `font.code`, the
                // one place a real monospace is still correct in this app.
                fontFamily: font.code,
                fontSize: ty.data,
                background: expanded === t.name ? c.selectBg : undefined,
              }}
            >
              <span
                onClick={() => {
                  const next = expanded === t.name ? null : t.name;
                  setExpanded(next);
                  setCols(next ? tableColumns(db, next) : []);
                }}
                className="quietbtn"
                style={{ cursor: "pointer", width: 10 }}
                title="Show columns"
              >
                <Caret dir={expanded === t.name ? "down" : "right"} size={8} />
              </span>
              <span
                onClick={() => browseTable(t.name)}
                // `.dimhover` steps a muted label up on approach. The table
                // name rests one notch below `text2` now so the hover has
                // somewhere to go — a control already at its brightest cannot
                // acknowledge the cursor.
                className="dimhover"
                style={{ cursor: "pointer", flex: 1 }}
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
                    fontFamily: font.code,
                    fontSize: ty.chip,
                    cursor: "pointer",
                  }}
                  className="quietbtn"
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
                  // `.dimhover` at rest; the LOADED query is blue inline, so
                  // it keeps saying so under the mouse.
                  className="dimhover"
                  style={{
                    border: "none",
                    background: "transparent",
                    color: name === q.name ? c.blue : undefined,
                    fontSize: ty.small,
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
                  className="cardbtn"
                  style={{
                    border: "none",
                    borderLeft: `1px solid ${c.lineStrong}`,
                    background: "transparent",
                    fontSize: ty.colhead,
                    padding: "5px 6px",
                    cursor: "pointer",
                  }}
                >
                  <Cross size={9} />
                </button>
              </span>
            ))}
            {queries.length === 0 && (
              <button className="actionbtn" onClick={resetToBuiltins} style={ghostBtn}>
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
              // THE ONE TEXTAREA IN THE APP THAT MUST BE MONOSPACE. Equal
              // LETTER widths are the point here, not equal digits: a sqlite
              // error names a character position, and indented joins only line
              // up in a fixed-pitch face.
              fontFamily: font.code,
              fontSize: ty.data,
              lineHeight: 1.5,
              outline: "none",
              resize: "vertical",
            }}
          />

          <div
            style={{ display: "flex", alignItems: "center", gap: 8, marginTop: 8, flexWrap: "wrap" }}
          >
            <button className="optimizebtn" onClick={() => run(sql)} style={primaryBtn}>
              Run
            </button>
            <span style={{ fontFamily: font.data, fontSize: ty.chip, color: c.dim }}>Ctrl+Enter</span>

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
                fontSize: ty.small,
                color: c.text,
                outline: "none",
                fontFamily: font.sans,
              }}
            />
            <button
              className="actionbtn"
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
                style={{ fontFamily: font.data, fontSize: ty.small, color: c.muted, marginLeft: "auto" }}
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
            <div style={{ padding: 16, color: c.red, fontFamily: font.code, fontSize: ty.data }}>
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

// Outlined for the same reason as the rail's Optimize: a filled accent slab is
// a permanent maximum. The two primaries must match, or whichever stays filled
// becomes the brightest thing in the app by default.
const primaryBtn: React.CSSProperties = {
  background: "transparent",
  border: `1px solid ${c.blue}`,
  color: c.blue,
  borderRadius: 4,
  padding: "7px 16px",
  fontSize: ty.data,
  fontWeight: weight.semi,
  cursor: "pointer",
  fontFamily: font.sans,
};

/** Geometry only — `.actionbtn` in index.css owns the border colour, the text
 *  colour and the hover, because an inline colour cannot be brightened by a
 *  `:hover` rule. Every `<button>` wearing this must also carry
 *  `className="actionbtn"`. */
const ghostBtn: React.CSSProperties = {
  // `borderStyle` + `borderWidth`, NEVER the `border` SHORTHAND, and this is
  // the third time the same trap has bitten this app (owner, Sep 2026: "the
  // outline to your Gen, Save, X buttons are brighter than the NFL").
  //
  // A shorthand writes ALL its longhands. `border: "1px solid"` names no colour,
  // so it sets `border-color: currentColor` — INLINE — which beats
  // `.actionbtn`'s `border-color` outright. The border then took the button's
  // TEXT colour (#d4d8de) instead of the intended #39404a, which is five steps
  // brighter and made a secondary button louder than the grid beside it.
  //
  // Naming only the two longhands the geometry needs leaves `border-color`
  // undeclared inline, so the stylesheet's value applies and the hover can
  // brighten it. Same family of bug as `background` on `.savedcard` and
  // `.optimizebtn`: the shorthand is the thing to distrust.
  borderStyle: "solid",
  borderWidth: 1,
  fontSize: ty.small,
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
        fontSize: ty.name,
      }}
    >
      {children}
    </div>
  );
}
