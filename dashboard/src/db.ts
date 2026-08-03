import initSqlJs from "sql.js";
import type { Database } from "sql.js";

/**
 * sql.js against the real data/golf.db — the Results Browser's data source.
 *
 * This is the one thing that genuinely needs a server. slate.js arrives via a
 * <script> tag, which file:// permits; a 20 MB binary cannot, so the whole DB
 * path is gated on servedOverHttp.
 *
 * The DB is NOT copied into dashboard/. It is already tracked in git at 20 MB,
 * and a weekly copy would add another 20 MB blob to history every week. The
 * server is rooted at the repo and serves the file in place.
 */

/** Candidates in order, so the app survives being served from either the repo
 *  root (the notebook's serve cell) or from dashboard/dist directly. */
const DB_CANDIDATES = [
  "/data/golf.db",
  "../../data/golf.db",
  "../data/golf.db",
  "./golf.db",
];

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
  throw new Error(`Could not load golf.db. Tried:\n  ${tried.join("\n  ")}`);
}

export function loadDatabase(): Promise<Database> {
  if (!dbPromise) {
    dbPromise = (async () => {
      const [SQL, buf] = await Promise.all([
        // The wasm is a separate file rather than inlined: it is only ever
        // needed when served over http, where fetching it is free.
        initSqlJs({ locateFile: () => new URL("sql-wasm.wasm", document.baseURI).href }),
        fetchFirst(DB_CANDIDATES),
      ]);
      const db = new SQL.Database(new Uint8Array(buf));
      // golf.db ships with no indexes — it is written by pandas.to_sql and
      // read by full scans everywhere else, so paying for them on disk would
      // only inflate a file that is already 20 MB in git. Build them here
      // instead: ~120 ms once per page load, in memory, never written back.
      //
      // Without ix_odds_k the Results Browser's per-row MIN(VEGAS_ODDS) would
      // scan the whole odds table 56,420 times. With it, that query drops from
      // 217 ms to 7 ms.
      db.run(`
        CREATE INDEX IF NOT EXISTS ix_odds_k
          ON odds(TOURNAMENT, ENDING_DATE, PLAYER);
        CREATE INDEX IF NOT EXISTS ix_tournaments_date
          ON tournaments(ENDING_DATE DESC, FINAL_POS);
      `);
      return db;
    })();
  }
  return dbPromise;
}

/** Ad-hoc SQL cap. Enough for exploration, small enough to lay out. */
export const ROW_LIMIT = 2000;
/** Whole-table browse cap. `tournaments` is ~56k rows and the point of the
 *  browse action is to hold all of it in memory so the column filters search
 *  the real table rather than a page of it. Only the first few hundred matches
 *  are ever rendered. */
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
  const r = runQuery(db, "SELECT DISTINCT SEASON FROM tournaments ORDER BY SEASON DESC", [], 500);
  return r.rows.map((row) => Number(row[0])).filter((n) => Number.isFinite(n));
}

/** Table names plus row counts, for the browser's schema sidebar. */
export function listTables(db: Database): { name: string; rows: number }[] {
  const out: { name: string; rows: number }[] = [];
  const stmt = db.prepare(
    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
  );
  const names: string[] = [];
  while (stmt.step()) names.push(stmt.get()[0] as string);
  stmt.free();
  for (const name of names) {
    const r = db.exec(`SELECT COUNT(*) FROM "${name}"`);
    out.push({ name, rows: (r[0]?.values?.[0]?.[0] as number) ?? 0 });
  }
  return out;
}

export function tableColumns(db: Database, table: string): string[] {
  const r = db.exec(`PRAGMA table_info("${table}")`);
  return (r[0]?.values ?? []).map((row) => String(row[1]));
}
