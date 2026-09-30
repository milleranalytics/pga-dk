import initSqlJs from "sql.js";
import type { Database } from "sql.js";

/**
 * sql.js against data/pga.db, the database pga-weekly.ipynb rebuilds from the
 * Tour's API — the Results Browser's and DB Query's data source. The slate
 * names it (meta.db); a slate that names none gets data/pga.db too.
 *
 * This is the one thing that genuinely needs a server. slate.js arrives via a
 * <script> tag, which file:// permits; a binary database cannot, so the whole DB
 * path is gated on servedOverHttp.
 *
 * The DB is NOT copied into dashboard/. The server is rooted at the repo and
 * serves the file in place.
 */

/** Candidates in order, so the app survives being served from either the repo
 *  root (the notebook's serve cell) or from dashboard/dist directly. */
const DB_FILE = "data/pga.db";
const candidates = (file: string) => [`/${file}`, `../../${file}`, `../${file}`];

export interface QueryResult {
  columns: string[];
  rows: (string | number | Uint8Array | null)[][];
  truncated: boolean;
}

let dbPromise: Promise<Database> | null = null;

async function fetchFirst(urls: string[]): Promise<ArrayBuffer> {
  const tried: string[] = [];
  for (const url of urls) {
    try {
      const res = await fetch(url);
      if (res.ok) {
        const buf = await res.arrayBuffer();
        // A dev server that rewrites unknown paths to index.html will happily
        // return 200 with HTML. SQLite files start with "SQLite format 3\0".
        const head = new TextDecoder().decode(new Uint8Array(buf.slice(0, 15)));
        if (head === "SQLite format 3") return buf;
        tried.push(`${url} (not a SQLite file)`);
        continue;
      }
      tried.push(`${url} (${res.status})`);
    } catch (e) {
      tried.push(`${url} (${(e as Error).message})`);
    }
  }
  throw new Error(`Could not load ${dbLabel()}. Tried:\n  ${tried.join("\n  ")}`);
}

/** The file name the tabs show while loading and in errors: "pga.db". */
export function dbLabel(): string {
  return (window.SLATE?.meta?.db ?? DB_FILE).split("/").pop() ?? "pga.db";
}

/**
 * A name with its accents taken off, for searching: "Højgaard" and "Jiménez"
 * match "hojgaard" and "jimenez". NFD splits é into e + accent; letters that
 * are not a letter plus an accent (ø, æ, ß) are mapped by hand.
 */
const LETTERS: Record<string, string> = {
  ø: "o", Ø: "O", æ: "ae", Æ: "AE", œ: "oe", Œ: "OE", ß: "ss", ł: "l", Ł: "L", đ: "d", Đ: "D", ı: "i",
};
export function fold(s: string | null): string | null {
  if (s == null) return null;
  return s
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[øØæÆœŒßłŁđĐı]/g, (ch) => LETTERS[ch]);
}

/**
 * v_results: pga.db's results with the names attached — one row per golfer per
 * event, with the event's date and course, his rounds, his pre-event odds and
 * his strokes gained per round there (ShotLink rounds only: blank at Augusta
 * and on a multi-course event's other courses). A TEMP view, built on every
 * load and never written to the file.
 *
 * Rounds, odds and SG are correlated subqueries, each an index seek, so a
 * query that filters or limits first pays for them only on the rows it keeps.
 * Sorting the WHOLE view pays for all 70k rows (about a second) — the Results
 * Browser therefore sorts on results/events and joins this view afterwards.
 */
const PGA_SETUP = `
CREATE TEMP VIEW v_results AS
SELECT e.season, e.end_date, r.tournament_id, e.name AS tournament, e.course,
       r.player_id, p.name AS player, r.position, r.finish_rank, r.to_par,
       ${[1, 2, 3, 4]
         .map(
           (n) =>
             `(SELECT x.strokes FROM rounds x WHERE x.tournament_id = r.tournament_id
                AND x.player_id = r.player_id AND x.round = ${n}) AS r${n}`,
         )
         .join(",\n       ")},
       o.decimal_minus_one AS odds,
       ${["sg_ott", "sg_app", "sg_arg", "sg_putt", "sg_total"]
         .map(
           (col) =>
             `(SELECT ROUND(AVG(s.${col}), 2) FROM sg_rounds s
                WHERE s.tournament_id = r.tournament_id AND s.player_id = r.player_id) AS ${col}`,
         )
         .join(",\n       ")}
FROM results r
JOIN events e ON e.tournament_id = r.tournament_id
JOIN players p ON p.player_id = r.player_id
LEFT JOIN odds o ON o.tournament_id = r.tournament_id AND o.player_id = r.player_id;
`;

/**
 * What the tabs need once per load, in memory, never written back. pga.db
 * carries its own indexes. fold() is registered on the connection, so DB Query
 * can use it too: WHERE fold(name) LIKE '%hojgaard%'.
 */
export function setupDatabase(db: Database): void {
  db.create_function("fold", fold);
  db.run(PGA_SETUP);
}

export function loadDatabase(): Promise<Database> {
  if (!dbPromise) {
    dbPromise = (async () => {
      const [SQL, buf] = await Promise.all([
        // The wasm is a separate file rather than inlined: it is only ever
        // needed when served over http, where fetching it is free.
        initSqlJs({ locateFile: () => new URL("sql-wasm.wasm", document.baseURI).href }),
        fetchFirst(candidates(window.SLATE?.meta?.db ?? DB_FILE)),
      ]);
      const db = new SQL.Database(new Uint8Array(buf));
      setupDatabase(db);
      return db;
    })();
  }
  return dbPromise;
}

/** Ad-hoc SQL cap. Enough for exploration, small enough to lay out. */
export const ROW_LIMIT = 2000;
/** Whole-table browse cap. `results` is ~70k rows and the point of the
 *  browse action is to hold all of it in memory so the column filters search
 *  the real table rather than a page of it; only `rounds` and `owgr` are
 *  bigger, and are cut at the cap (query those with a WHERE). Only the first
 *  few hundred matches are ever rendered. */
export const BROWSE_LIMIT = 200000;

export type BindValue = string | number | null;

export function runQuery(
  db: Database,
  sql: string,
  params?: BindValue[],
  limit: number = ROW_LIMIT,
): QueryResult {
  const stmt = db.prepare(sql);
  try {
    if (params?.length) stmt.bind(params);
    const columns = stmt.getColumnNames();
    const rows: QueryResult["rows"] = [];
    let truncated = false;
    while (stmt.step()) {
      if (rows.length >= limit) {
        truncated = true;
        break;
      }
      rows.push(stmt.get() as QueryResult["rows"][number]);
    }
    // getColumnNames() is empty until the first step on some statements.
    return { columns: columns.length ? columns : stmt.getColumnNames(), rows, truncated };
  } finally {
    stmt.free();
  }
}

/** First column of the first row, for COUNT(*)-shaped queries. */
export function scalar(db: Database, sql: string, params?: BindValue[]): number {
  const r = runQuery(db, sql, params, 1);
  const v = r.rows[0]?.[0];
  return typeof v === "number" ? v : 0;
}

export function distinctSeasons(db: Database): number[] {
  const sql = `SELECT DISTINCT e.season FROM events e
    WHERE EXISTS (SELECT 1 FROM results r WHERE r.tournament_id = e.tournament_id)
    ORDER BY 1 DESC`;
  const r = runQuery(db, sql, [], 500);
  return r.rows.map((row) => Number(row[0])).filter((n) => Number.isFinite(n));
}

/** Tables plus row counts, for the browser's schema sidebar; then the views
 *  built in the browser at load (pga.db's v_results), marked as views. */
export function listTables(db: Database): { name: string; rows: number; view: boolean }[] {
  const out: { name: string; rows: number; view: boolean }[] = [];
  const stmt = db.prepare(
    `SELECT name, 0 FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'
     UNION ALL
     SELECT name, 1 FROM sqlite_temp_master WHERE type='view'
     ORDER BY 2, 1`,
  );
  const names: [string, boolean][] = [];
  while (stmt.step()) {
    const [name, view] = stmt.get();
    names.push([name as string, view === 1]);
  }
  stmt.free();
  for (const [name, view] of names) {
    const r = db.exec(`SELECT COUNT(*) FROM "${name}"`);
    out.push({ name, rows: (r[0]?.values?.[0]?.[0] as number) ?? 0, view });
  }
  return out;
}

export function tableColumns(db: Database, table: string): string[] {
  const r = db.exec(`PRAGMA table_info("${table}")`);
  return (r[0]?.values ?? []).map((row) => String(row[1]));
}
