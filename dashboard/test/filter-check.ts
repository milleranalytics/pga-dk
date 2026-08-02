// Column-filter predicates, checked against the value FORMATS this database
// actually stores — several numeric columns are text ("E" for even par,
// "$1,584,000.00", "57.98%"), so a naive Number() would make comparison
// filters silently match nothing on exactly the columns worth comparing.
//
// Run: npm run test:filters

import { buildPredicate, toNumber } from "../src/components/ResultTable";

let failures = 0;

function check(label: string, got: unknown, want: unknown) {
  const ok = got === want;
  if (!ok) {
    failures++;
    console.log(`  FAIL  ${label}\n        got ${JSON.stringify(got)}, want ${JSON.stringify(want)}`);
  }
}

function match(filter: string, cell: string | number | null): boolean {
  const p = buildPredicate(filter);
  return p ? p(cell) : true;
}

console.log("toNumber — the formats in golf.db");
check("plain number", toNumber(-7), -7);
check("numeric string", toNumber("-7"), -7);
check("even par E", toNumber("E"), 0);
check("lowercase e", toNumber("e"), 0);
check("money", toNumber("$1,584,000.00"), 1584000);
check("percent", toNumber("57.98%"), 57.98);
check("blank", toNumber(""), null);
check("null", toNumber(null), null);
check("non-numeric text", toNumber("CUT"), null);
check("T-position is not a number", toNumber("T26"), null);

console.log("substring (default)");
check("case-insensitive", match("hoj", "Nicolai Hojgaard"), true);
check("no match", match("xyz", "Nicolai Hojgaard"), false);
check("matches inside numbers", match("7", "-70"), true);
check("null cell", match("a", null), false);

console.log("numeric comparison");
check("> greater", match(">20", 30), true);
check("> equal is false", match(">20", 20), false);
check(">= equal is true", match(">=20", 20), true);
check("< less", match("<5", -7), true);
check("<= boundary", match("<=3", 3), true);
check("= exact", match("=7", 7), true);
check("= not equal", match("=7", 8), false);
check("!= operator", match("!=0", 5), true);
check("!= excludes", match("!=0", 0), false);
check("<> alias", match("<>0", 5), true);
check("negative threshold", match("<-5", -7), true);
check("negative threshold excludes", match("<-5", -3), false);
check("spaces allowed", match(">  20", 30), true);

console.log("numeric comparison against text-stored values");
check("round score as text", match("<-5", "-7"), true);
check("even par vs >-1", match(">-1", "E"), true);
check("even par vs >0", match(">0", "E"), false);
check("money threshold", match(">1000000", "$1,584,000.00"), true);
check("percent threshold", match(">=50", "57.98%"), true);

console.log("non-numeric cells never satisfy a comparison");
check("CUT vs >0", match(">0", "CUT"), false);
check("CUT vs <100", match("<100", "CUT"), false);
check("null vs >0", match(">0", null), false);
check("blank vs <100", match("<100", ""), false);

console.log("ranges");
check("inside", match("10..20", 15), true);
check("low boundary", match("10..20", 10), true);
check("high boundary", match("10..20", 20), true);
check("outside", match("10..20", 21), false);
check("reversed bounds still work", match("20..10", 15), true);
check("negative range", match("-10..-5", -7), true);
check("range on text value", match("-10..-5", "-7"), true);

console.log("things that must stay text");
check("bare number is substring", match("20", "1201"), true);
check("date-like is substring", match("2026-07", "2026-07-26"), true);
check("empty filter matches all", match("", "anything"), true);
check("whitespace-only matches all", match("   ", "anything"), true);

console.log(failures === 0 ? "\nPASS — all filter cases behave." : `\nFAIL — ${failures} case(s).`);
process.exit(failures === 0 ? 0 : 1);
