// How many flags does a player actually get, and which rules do the firing?
//
// "Too many flags" is a measurable claim, so measure it rather than eyeball a
// few cards. Run: npm run census
//
// Reads the real weekly slate, so the numbers describe this week's field.

import { readFileSync } from "node:fs";
import { enrich } from "../src/enrich";
import { playerFlags } from "../src/flags";
import type { Slate } from "../src/types";

const raw = readFileSync("public/data/slate.js", "utf8");
const slate = JSON.parse(raw.slice(raw.indexOf("{"), raw.lastIndexOf(";"))) as Slate;
const field = enrich(slate);

/** Collapse a rendered message back to the rule that produced it. */
function ruleOf(text: string): string {
  const head = text.split(/[:—(]/)[0].trim();
  return head
    .replace(/\s+(driving|approach|around-green|putting)$/, " <phase>")
    .replace(/ at .+$/, " at <course>")
    .replace(/\d+/g, "N");
}

const counts: number[] = [];
const bySeverity = new Map<string, number>();
const byRule = new Map<string, number>();

for (const p of field.players) {
  const flags = playerFlags(p, field);
  counts.push(flags.length);
  for (const fl of flags) {
    bySeverity.set(fl.severity, (bySeverity.get(fl.severity) ?? 0) + 1);
    const r = ruleOf(fl.text);
    byRule.set(r, (byRule.get(r) ?? 0) + 1);
  }
}

counts.sort((a, b) => a - b);
const total = counts.reduce((a, b) => a + b, 0);
const mean = total / counts.length;
const median = counts[Math.floor(counts.length / 2)];

console.log(`field: ${field.players.length} players · ${total} flags total`);
console.log(`per player — mean ${mean.toFixed(1)}, median ${median}, min ${counts[0]}, max ${counts[counts.length - 1]}`);

const hist = new Map<number, number>();
for (const n of counts) hist.set(n, (hist.get(n) ?? 0) + 1);
console.log("\nflags per player:");
for (const n of [...hist.keys()].sort((a, b) => a - b)) {
  console.log(`  ${String(n).padStart(2)} flags  ${"█".repeat(hist.get(n)!)} ${hist.get(n)}`);
}

console.log("\nby severity:");
for (const [s, n] of [...bySeverity.entries()].sort((a, b) => b[1] - a[1])) {
  console.log(`  ${s.padEnd(6)} ${String(n).padStart(4)}`);
}

console.log("\nby rule (how often each fires across the field):");
for (const [r, n] of [...byRule.entries()].sort((a, b) => b[1] - a[1])) {
  const pctOfField = ((n / field.players.length) * 100).toFixed(0);
  console.log(`  ${String(n).padStart(4)}  ${pctOfField.padStart(3)}%  ${r}`);
}
