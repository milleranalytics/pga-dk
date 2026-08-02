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
      return new SQL.Database(new Uint8Array(buf));
    })();
  }
  return dbPromise;
}

const ROW_LIMIT = 2000;

export function runQuery(db: Database, sql: string): QueryResult {
  const stmt = db.prepare(sql);
  try {
    const columns = stmt.getColumnNames();
    const rows: QueryResult["rows"] = [];
    let truncated = false;
    while (stmt.step()) {
      if (rows.length >= ROW_LIMIT) {
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
