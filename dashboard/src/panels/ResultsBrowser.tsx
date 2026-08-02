import { useCallback, useEffect, useMemo, useState } from "react";
import type { Database } from "sql.js";
import { c, font } from "../tokens";
import { loadDatabase, runQuery, scalar, distinctSeasons, ROW_LIMIT } from "../db";
import type { QueryResult, BindValue } from "../db";
import ResultTable from "../components/ResultTable";

/**
 * Results Browser — the tournaments table with the filters used most often,
 * ported from the Streamlit app.
 *
 * The DB Query tab can express anything, but "show me every Hojgaard round at
 * Birkdale" should not require writing a join. This is that path: four filters,
 * odds already joined, newest event first.
 *
 * Filters are BOUND parameters, never string-interpolated. A player named
 * O'Connor would otherwise break the SQL, and the LIKE patterns come straight
 * from user input.
 */

const SELECT = `
SELECT t.SEASON            AS Season,
       t.ENDING_DATE       AS Ends,
       t.TOURNAMENT        AS Tournament,
       t.COURSE            AS Course,
       t.PLAYER            AS Player,
       t.POS               AS Pos,
       o.VEGAS_ODDS        AS "Odds (/1)",
       t."ROUNDS:1"        AS R1,
       t."ROUNDS:2"        AS R2,
       t."ROUNDS:3"        AS R3,
       t."ROUNDS:4"        AS R4
FROM tournaments t
-- Pre-aggregated so a duplicated odds row cannot multiply result rows. The
-- odds table is deduped in the Python pipeline but not in the file itself.
LEFT JOIN (
  SELECT TOURNAMENT, ENDING_DATE, PLAYER, MIN(VEGAS_ODDS) AS VEGAS_ODDS
  FROM odds GROUP BY TOURNAMENT, ENDING_DATE, PLAYER
) o ON o.TOURNAMENT = t.TOURNAMENT
   AND o.ENDING_DATE = t.ENDING_DATE
   AND o.PLAYER = t.PLAYER`;

interface Filters {
  player: string;
  tournament: string;
  course: string;
  seasons: number[];
}

/** WHERE clause + bound values, shared by the data query and the count. */
function buildWhere(f: Filters): { where: string; params: BindValue[] } {
  const parts: string[] = [];
  const params: BindValue[] = [];
  if (f.player.trim()) {
    parts.push("t.PLAYER LIKE ?");
    params.push(`%${f.player.trim()}%`);
  }
  if (f.tournament.trim()) {
    parts.push("t.TOURNAMENT LIKE ?");
    params.push(`%${f.tournament.trim()}%`);
  }
  if (f.course.trim()) {
    parts.push("t.COURSE LIKE ?");
    params.push(`%${f.course.trim()}%`);
  }
  if (f.seasons.length) {
    parts.push(`t.SEASON IN (${f.seasons.map(() => "?").join(",")})`);
    params.push(...f.seasons);
  }
  return { where: parts.length ? `WHERE ${parts.join(" AND ")}` : "", params };
}

export default function ResultsBrowser() {
  const [db, setDb] = useState<Database | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [seasons, setSeasons] = useState<number[]>([]);
  const [seasonsOpen, setSeasonsOpen] = useState(false);

  const [filters, setFilters] = useState<Filters>({
    player: "",
    tournament: "",
    course: "",
    seasons: [],
  });
  const [result, setResult] = useState<QueryResult | null>(null);
  const [matching, setMatching] = useState(0);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    loadDatabase()
      .then((d) => {
        if (cancelled) return;
        setDb(d);
        setSeasons(distinctSeasons(d));
      })
      .catch((e: Error) => !cancelled && setLoadError(e.message));
    return () => {
      cancelled = true;
    };
  }, []);

  const search = useCallback(
    (f: Filters) => {
      if (!db) return;
      const { where, params } = buildWhere(f);
      try {
        // Newest event first; within an event, winners at the top. CUT/WD sink
        // because FINAL_POS is 90-filled for them.
        const sql = `${SELECT}\n${where}\nORDER BY t.ENDING_DATE DESC, t.FINAL_POS ASC`;
        setResult(runQuery(db, sql, params, ROW_LIMIT));
        setMatching(scalar(db, `SELECT COUNT(*) FROM tournaments t ${where}`, params));
        setError(null);
      } catch (e) {
        setError((e as Error).message);
        setResult(null);
      }
    },
    [db],
  );

  // Debounced so typing a name does not fire a query per keystroke.
  useEffect(() => {
    if (!db) return;
    const id = setTimeout(() => search(filters), 220);
    return () => clearTimeout(id);
  }, [db, filters, search]);

  const seasonLabel = useMemo(() => {
    if (!filters.seasons.length) return "All seasons";
    if (filters.seasons.length === 1) return String(filters.seasons[0]);
    return `${filters.seasons.length} seasons`;
  }, [filters.seasons]);

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

  const set = (patch: Partial<Filters>) => setFilters((f) => ({ ...f, ...patch }));

  return (
    <div style={{ flex: 1, display: "flex", flexDirection: "column", minHeight: 0 }}>
      <div style={{ padding: "12px 16px", borderBottom: `1px solid ${c.line}` }}>
        <div style={{ display: "flex", gap: 12, flexWrap: "wrap", alignItems: "flex-end" }}>
          <Field label="PLAYER CONTAINS">
            <input
              value={filters.player}
              onChange={(e) => set({ player: e.target.value })}
              placeholder="e.g. Hojgaard"
              style={inputStyle}
            />
          </Field>
          <Field label="TOURNAMENT CONTAINS">
            <input
              value={filters.tournament}
              onChange={(e) => set({ tournament: e.target.value })}
              placeholder="e.g. Deere"
              style={inputStyle}
            />
          </Field>
          <Field label="COURSE CONTAINS">
            <input
              value={filters.course}
              onChange={(e) => set({ course: e.target.value })}
              placeholder="e.g. Birkdale"
              style={inputStyle}
            />
          </Field>
          <Field label="SEASONS">
            <div style={{ position: "relative" }}>
              <button
                onClick={() => setSeasonsOpen((o) => !o)}
                style={{ ...inputStyle, textAlign: "left", cursor: "pointer" }}
              >
                {seasonLabel} ▾
              </button>
              {seasonsOpen && (
                <div
                  style={{
                    position: "absolute",
                    top: "100%",
                    left: 0,
                    zIndex: 20,
                    marginTop: 4,
                    background: c.surface,
                    border: `1px solid ${c.lineStrong}`,
                    borderRadius: 4,
                    maxHeight: 260,
                    overflowY: "auto",
                    minWidth: 160,
                    padding: 4,
                  }}
                >
                  <div
                    onClick={() => set({ seasons: [] })}
                    style={{ ...seasonRow, color: c.dim, borderBottom: `1px solid ${c.lineSoft}` }}
                  >
                    All seasons
                  </div>
                  {seasons.map((s) => {
                    const on = filters.seasons.includes(s);
                    return (
                      <div
                        key={s}
                        onClick={() =>
                          set({
                            seasons: on
                              ? filters.seasons.filter((x) => x !== s)
                              : [...filters.seasons, s],
                          })
                        }
                        style={{ ...seasonRow, color: on ? c.green : c.text2 }}
                      >
                        {on ? "✓ " : "   "}
                        {s}
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          </Field>

          <button
            onClick={() => set({ player: "", tournament: "", course: "", seasons: [] })}
            style={{
              border: `1px solid ${c.lineStrong}`,
              background: "transparent",
              color: c.text2,
              fontSize: 11.5,
              padding: "7px 12px",
              borderRadius: 4,
              cursor: "pointer",
              fontFamily: font.sans,
            }}
          >
            Clear
          </button>
        </div>

        <div style={{ marginTop: 9, fontFamily: font.mono, fontSize: 10.5, color: c.dim }}>
          {matching.toLocaleString()} matching row{matching === 1 ? "" : "s"}
          {matching > ROW_LIMIT && (
            <span style={{ color: c.amber }}> · showing the first {ROW_LIMIT.toLocaleString()}</span>
          )}{" "}
          · blank odds = player not listed / name mismatch
        </div>
      </div>

      <div style={{ flex: 1, overflow: "auto", minHeight: 0 }}>
        {error ? (
          <div style={{ padding: 16, color: c.red, fontFamily: font.mono, fontSize: 12 }}>
            {error}
          </div>
        ) : result ? (
          // Column filters are off here: the four facets above already cover
          // this table, and a second filter row that searches only the loaded
          // 2,000 rows would quietly disagree with them.
          <ResultTable
            result={result}
            emptyText="No rows match these filters."
            columnFilters={false}
          />
        ) : null}
      </div>
    </div>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
      <span
        style={{
          fontFamily: font.mono,
          fontSize: 9,
          letterSpacing: "0.1em",
          color: c.dim,
        }}
      >
        {label}
      </span>
      {children}
    </div>
  );
}

const inputStyle: React.CSSProperties = {
  width: 190,
  background: c.surfaceAlt,
  border: `1px solid ${c.lineStrong}`,
  borderRadius: 4,
  padding: "7px 10px",
  fontSize: 12,
  color: c.text,
  outline: "none",
  fontFamily: font.sans,
};

const seasonRow: React.CSSProperties = {
  padding: "4px 9px",
  fontFamily: font.mono,
  fontSize: 11.5,
  cursor: "pointer",
  borderRadius: 3,
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
