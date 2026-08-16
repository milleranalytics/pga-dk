// What does the one ramp actually paint on this week's slate?
//
// "Colour has lost its meaning because everything is coloured" is a measurable
// claim, so measure it rather than eyeball a screenshot. Run: npm run ramp
//
// Checks three things:
//  1. The band census per grid column — how much of the grid is hued at all.
//  2. That equal values always get equal colour (the tie invariant). This is
//     the one that would fail silently and look like a rendering glitch.
//  3. That the ramp is monotone in percentile — brighter/greener never means
//     "further down the field".

import { readFileSync } from "node:fs";
import { enrich, type Metric } from "../src/enrich";
import { metricValue } from "../src/enrich";
import { c, rankColor, finishColor } from "../src/tokens";
import type { Slate } from "../src/types";

const raw = readFileSync("public/data/slate.js", "utf8");
const slate = JSON.parse(raw.slice(raw.indexOf("{"), raw.lastIndexOf(";"))) as Slate;
const field = enrich(slate);

// One band per quintile, plus the absence state. c.text is absent on purpose —
// it is the player-name colour and is not part of the ramp, so if it ever shows
// up in this census something has put a number at name brightness.
const BAND: [string, string][] = [
  [c.green, "green"],
  [c.text2, "text2"],
  [c.muted, "muted"],
  [c.dim, "dim"],
  [c.red, "red"],
  [c.dimmer, "dimmer"],
];
const bandName = (hex: string) => BAND.find((b) => b[0] === hex)?.[1] ?? hex;

// Every grid column that is on the ramp. SALARY and EXP are the documented
// exceptions and are deliberately absent.
const COLUMNS: Metric[] = [
  "P_TOP20",
  "VAL",
  "LEVERAGE",
  "VEGAS_ODDS",
  "SG_FORM",
  "SG_CH_SHRUNK",
  "CUT_PERCENTAGE",
  "OWGR_RANK",
];

let fail = 0;
const bad = (msg: string) => {
  console.log(`  FAIL — ${msg}`);
  fail++;
};

// --- 1. band census ---------------------------------------------------------

console.log(`${slate.meta.tournament} — ${field.players.length} players\n`);
console.log("column          green text2 muted   dim   red dimmer");

const total = new Map<string, number>();
for (const m of COLUMNS) {
  const row = new Map<string, number>();
  for (const p of field.players) {
    const n = bandName(rankColor(field.pct[m][p.id]));
    row.set(n, (row.get(n) ?? 0) + 1);
    total.set(n, (total.get(n) ?? 0) + 1);
  }
  const cells = BAND.map(([, n]) => String(row.get(n) ?? 0).padStart(5)).join(" ");
  console.log(`${m.padEnd(15)} ${cells}`);
}

const cellCount = COLUMNS.length * field.players.length;
const hued = (total.get("green") ?? 0) + (total.get("red") ?? 0);
console.log(
  `\nhued: ${hued} of ${cellCount} ramped cells (${((hued / cellCount) * 100).toFixed(0)}%)` +
    ` — green ${total.get("green") ?? 0}, red ${total.get("red") ?? 0}`,
);

// --- 2. the tie invariant ---------------------------------------------------
// INVARIANT: two players with the SAME raw value in a column must get the same
// percentile, the same printed rank and therefore the same colour. Without it a
// slate with a dozen players at 60/1 paints identical cells different colours.

console.log("\nequal values get equal colour");
for (const m of COLUMNS) {
  const byValue = new Map<number, string[]>();
  for (const p of field.players) {
    const v = metricValue(p, m);
    if (v === null) continue;
    byValue.set(v, [...(byValue.get(v) ?? []), p.id]);
  }
  let tiedGroups = 0;
  let tiedPlayers = 0;
  for (const [v, ids] of byValue) {
    if (ids.length < 2) continue;
    tiedGroups++;
    tiedPlayers += ids.length;
    const pcts = new Set(ids.map((id) => field.pct[m][id]));
    const rnks = new Set(ids.map((id) => field.rnk[m][id]));
    if (pcts.size !== 1) bad(`${m} value ${v}: ${pcts.size} different percentiles`);
    if (rnks.size !== 1) bad(`${m} value ${v}: ${rnks.size} different ranks`);
  }
  console.log(`  ${m.padEnd(15)} ${tiedGroups} tied groups covering ${tiedPlayers} players`);
}

// --- 3. monotonicity --------------------------------------------------------
// INVARIANT: the colour is a non-decreasing function of percentile. Walk the
// field sorted by percentile and assert the band index never moves backwards.

console.log("\nthe ramp never goes backwards");
const ORDER = ["red", "dim", "muted", "text2", "green"];
for (const m of COLUMNS) {
  const ranked = field.players
    .filter((p) => field.pct[m][p.id] !== undefined)
    .sort((a, b) => field.pct[m][a.id] - field.pct[m][b.id]);
  let prev = -1;
  for (const p of ranked) {
    const i = ORDER.indexOf(bandName(rankColor(field.pct[m][p.id])));
    if (i < prev) bad(`${m}: ${p.id} at pct ${field.pct[m][p.id]} moved backwards`);
    prev = Math.max(prev, i);
  }
}
// The same for finishes, whose percentile is synthesized rather than ranked.
let prevF = 6;
for (let n = 1; n <= 200; n++) {
  const i = ORDER.indexOf(bandName(finishColor(n)));
  if (i > prevF) bad(`finishColor: place ${n} scored better than place ${n - 1}`);
  prevF = i;
}
if (finishColor(1) !== c.green) bad("finishColor: a win is not green");
if (finishColor(200) !== c.red) bad("finishColor: dead last is not red");
if (finishColor(null) !== c.dimmer) bad("finishColor: no finish is not an absence");
console.log("  every column, plus finishColor over places 1..200");

// Where the finish bands actually break, so the boundaries are auditable.
const edges: string[] = [];
let cur = "";
for (let n = 1; n <= 160; n++) {
  const b = bandName(finishColor(n));
  if (b !== cur) {
    edges.push(`${b} from ${n}`);
    cur = b;
  }
}
console.log(`\nfinish bands: ${edges.join(", ")}`);

console.log(fail === 0 ? "\nPASS — the ramp is consistent." : `\n${fail} FAILURE(S)`);
process.exit(fail === 0 ? 0 : 1);
