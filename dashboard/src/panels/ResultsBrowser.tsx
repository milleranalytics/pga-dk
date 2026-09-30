import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { Database } from "sql.js";
import { c, font, type as t } from "../tokens";
import { Caret, Check } from "../components/icons";
import { loadDatabase, runQuery, scalar, distinctSeasons, dbLabel, ROW_LIMIT } from "../db";
import { fmtSigned } from "../format";
import type { QueryResult, BindValue } from "../db";
import ResultTable from "../components/ResultTable";

/**
 * Results Browser — every golfer's result in every event, with the filters
 * used most often, ported from the Streamlit app: pga.db's results with names,
 * rounds, odds and strokes gained.
 *
 * The DB Query tab can express anything, but "show me every Hojgaard round at
 * Birkdale" should not require writing a join. This is that path: four filters,
 * odds already joined, newest event first.
 *
 * Filters are BOUND parameters, never string-interpolated. A player named
 * O'Connor would otherwise break the SQL, and the LIKE patterns come straight
 * from user input.
 */

interface Filters {
  player: string;
  tournament: string;
  course: string;
  seasons: number[];
}

/**
 * The four filters run on results + events, then v_results (db.ts) is joined
 * to the rows kept. Sorting v_results itself would
 * work out every golfer-event's rounds, odds and SG before the LIMIT — about
 * a second — so the sort and the limit run on the two cheap tables and only
 * the 2,000 rows shown pay for the rest.
 *
 * The player filter folds accents on both sides ("hojgaard" finds Højgaard),
 * over the 4k-row players table rather than per result row.
 */
export const PGA_FROM = `FROM results r JOIN events e ON e.tournament_id = r.tournament_id`;

export function pgaWhere(f: Filters): { where: string; params: BindValue[] } {
  const parts: string[] = [];
  const params: BindValue[] = [];
  if (f.player.trim()) {
    parts.push("r.player_id IN (SELECT player_id FROM players WHERE fold(name) LIKE fold(?))");
    params.push(`%${f.player.trim()}%`);
  }
  if (f.tournament.trim()) {
    parts.push("e.name LIKE ?");
    params.push(`%${f.tournament.trim()}%`);
  }
  if (f.course.trim()) {
    parts.push("e.course LIKE ?");
    params.push(`%${f.course.trim()}%`);
  }
  if (f.seasons.length) {
    parts.push(`e.season IN (${f.seasons.map(() => "?").join(",")})`);
    params.push(...f.seasons);
  }
  return { where: parts.length ? `WHERE ${parts.join(" AND ")}` : "", params };
}

/** Newest event first, winners at the top; a missed cut has no finish_rank, so it sinks. */
export function pgaQuery(where: string): string {
  return `
WITH k AS (
  SELECT r.tournament_id, r.player_id, e.end_date, r.finish_rank ${PGA_FROM}
  ${where}
  ORDER BY e.end_date DESC, r.finish_rank IS NULL, r.finish_rank
  LIMIT ${ROW_LIMIT + 1})
SELECT v.season      AS Season,
       v.end_date    AS Ends,
       v.tournament  AS Tournament,
       v.course      AS Course,
       v.player      AS Player,
       v.position    AS Pos,
       v.odds        AS "Odds (/1)",
       v.r1 AS R1, v.r2 AS R2, v.r3 AS R3, v.r4 AS R4,
       v.sg_ott      AS "SG OTT",
       v.sg_app      AS "SG APP",
       v.sg_arg      AS "SG ARG",
       v.sg_putt     AS "SG PUTT",
       v.sg_total    AS "SG TOT"
FROM k JOIN v_results v ON v.tournament_id = k.tournament_id AND v.player_id = k.player_id
ORDER BY k.end_date DESC, k.finish_rank IS NULL, k.finish_rank`;
}

/** Strokes gained per round, signed to two places as everywhere else in the app. */
const sg = (v: number) => fmtSigned(v, 2);
const PGA_FORMATS = { "SG OTT": sg, "SG APP": sg, "SG ARG": sg, "SG PUTT": sg, "SG TOT": sg };

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
      try {
        const { where, params } = pgaWhere(f);
        setResult(runQuery(db, pgaQuery(where), params, ROW_LIMIT));
        setMatching(scalar(db, `SELECT COUNT(*) ${PGA_FROM} ${where}`, params));
        setError(null);
      } catch (e) {
        setError((e as Error).message);
        setResult(null);
      }
    },
    [db],
  );

  // Debounced so typing a name does not fire a query per keystroke — but NOT
  // on the first run, where there is nothing to debounce and the delay is just
  // 220 ms of empty panel between clicking the tab and seeing rows.
  const primed = useRef(false);
  useEffect(() => {
    if (!db) return;
    if (!primed.current) {
      primed.current = true;
      search(filters);
      return;
    }
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
        <div style={{ color: c.red, fontFamily: font.data, fontSize: t.data, whiteSpace: "pre-wrap" }}>
          {loadError}
        </div>
      </Centered>
    );
  }
  if (!db) return <Centered>Loading {dbLabel()}…</Centered>;

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
                {seasonLabel}
                <Caret dir="down" size={8} />
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
                        // `.dimhover` at rest; a SELECTED season is blue
                        // inline and keeps its colour under the mouse.
                        className="dimhover"
                        style={{ ...seasonRow, color: on ? c.blue : undefined }}
                      >
                        {/* A DRAWN TICK IN A HELD BOX. It was `✓` against two
                            no-break spaces — a glyph Archivo does not carry,
                            balanced against spaces that are not the same width
                            in a proportional face, so an unselected season sat
                            a pixel or two off its selected neighbour. The box
                            is always there and only its contents change, which
                            is the rule the grid's L/X icons already follow. */}
                        <span
                          style={{
                            width: 13,
                            flex: "none",
                            display: "inline-flex",
                            alignItems: "center",
                          }}
                        >
                          {on && <Check size={9} />}
                        </span>
                        {s}
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          </Field>

          <button
            className="actionbtn"
            onClick={() => set({ player: "", tournament: "", course: "", seasons: [] })}
            style={{
              // See `secondaryBtn` in LineupRail: the `border` shorthand would
              // set `border-color: currentColor` inline and out-rank
              // `.actionbtn`'s, painting the edge in the text colour.
              borderStyle: "solid",
              borderWidth: 1,
              fontSize: t.small,
              padding: "7px 12px",
              borderRadius: 4,
              cursor: "pointer",
              fontFamily: font.sans,
            }}
          >
            Clear
          </button>
        </div>

        <div style={{ marginTop: 9, fontFamily: font.data, fontSize: t.chip, color: c.dim }}>
          {matching.toLocaleString()} matching row{matching === 1 ? "" : "s"}
          {matching > ROW_LIMIT && (
            <span style={{ color: c.amber }}> · showing the first {ROW_LIMIT.toLocaleString()}</span>
          )}{" "}
          · blank odds = no board saved for that event · SG = per round there, ShotLink
          rounds only
        </div>
      </div>

      <div style={{ flex: 1, overflow: "auto", minHeight: 0 }}>
        {error ? (
          <div style={{ padding: 16, color: c.red, fontFamily: font.data, fontSize: t.data }}>
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
            formats={PGA_FORMATS}
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
          fontFamily: font.data,
          fontSize: t.micro,
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
  fontSize: t.data,
  color: c.text,
  outline: "none",
  fontFamily: font.sans,
};

const seasonRow: React.CSSProperties = {
  // A flex line, because the tick is a drawn box beside the label rather than a
  // character in front of it.
  display: "flex",
  alignItems: "center",
  gap: 4,
  padding: "4px 9px",
  fontFamily: font.data,
  fontSize: t.data,
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
        fontSize: t.name,
      }}
    >
      {children}
    </div>
  );
}
