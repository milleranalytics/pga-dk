import type { Field, Player, Metric } from "./enrich";
import { fmtSigned } from "./format";

/**
 * The flag engine — an automatic scan for where a player is an OUTLIER against
 * the CURRENT FIELD, so two players can be compared without reading every
 * number. Thresholds are ported as-is from the design handoff; they are tuned.
 *
 * Two rules the handoff is emphatic about, both honoured here:
 *  - Every message embeds the actual VALUE and the field RANK. The verdict
 *    alone is not the point; the number is.
 *  - Consistency: any two numbers describing the same quantity must come from
 *    the same source. Every figure below is read from the same enriched field
 *    that feeds the grid and the percentile bars — nothing is recomputed on a
 *    different window, which is how "100% cuts made" ended up above
 *    "CUTS /20 95%" in an early prototype build.
 */

export type Severity = "good" | "bad" | "warn" | "info" | "none";

export interface Flag {
  severity: Severity;
  text: string;
}

/** "top 6% of field" — how far into the field this percentile sits. */
function topPct(p: number): number {
  return Math.max(1, Math.round((1 - p) * 100));
}

function bottomPct(p: number): number {
  return Math.max(1, Math.round(p * 100));
}

const PHASES: { key: Metric; label: string }[] = [
  { key: "ott", label: "driving" },
  { key: "app", label: "approach" },
  { key: "arg", label: "around-green" },
  { key: "putt", label: "putting" },
];

export function playerFlags(p: Player, f: Field): Flag[] {
  const out: Flag[] = [];
  const pct = (m: Metric) => f.pct[m][p.id];
  const rnk = (m: Metric) => f.rnk[m][p.id];
  const N = f.players.length;

  // --- model / value ---
  if (pct("P_TOP20") >= 0.92) {
    out.push({
      severity: "good",
      text: `Model love: ${(p.P_TOP20 * 100).toFixed(1)}% top-20 — rank ${rnk("P_TOP20")} of ${N}`,
    });
  }
  if (pct("VAL") >= 0.88) {
    out.push({
      severity: "good",
      text: `Value play: ${p.VAL.toFixed(2)} P20%/$1k — rank ${rnk("VAL")} of ${N}`,
    });
  } else if (pct("VAL") <= 0.12) {
    out.push({
      severity: "bad",
      text: `Poor value: ${p.VAL.toFixed(2)} P20%/$1k — rank ${rnk("VAL")} of ${N}`,
    });
  }

  // --- form ---
  if (pct("SG_FORM") >= 0.85) {
    out.push({
      severity: "good",
      text: `Hot form: SG ${fmtSigned(p.SG_FORM, 2)}/rd — top ${topPct(pct("SG_FORM"))}% of field`,
    });
  } else if (pct("SG_FORM") <= 0.15) {
    out.push({
      severity: "bad",
      text: `Cold form: SG ${fmtSigned(p.SG_FORM, 2)}/rd — rank ${rnk("SG_FORM")} of ${N}`,
    });
  }

  // --- reliability ---
  if (p.CUT_PERCENTAGE >= 95) {
    out.push({ severity: "good", text: `Steady: ${p.CUT_PERCENTAGE.toFixed(0)}% cuts made` });
  } else if (p.CUT_PERCENTAGE <= 62) {
    out.push({ severity: "warn", text: `Volatile: ${p.CUT_PERCENTAGE.toFixed(0)}% cuts made` });
  }

  // --- course ---
  // Exactly 0 means NO history, not neutral history. Checked first so a genuine
  // 0.0 can never fall through to the "poor fit" branch.
  if (p.SG_CH_SHRUNK === 0) {
    out.push({ severity: "warn", text: `No course history at ${f.meta.course}` });
  } else if (p.SG_CH_SHRUNK >= 0.8) {
    out.push({
      severity: "good",
      text: `Course fit ${fmtSigned(p.SG_CH_SHRUNK, 2)} at ${f.meta.course}`,
    });
  } else if (p.SG_CH_SHRUNK <= -0.5) {
    out.push({
      severity: "bad",
      text: `Course history ${fmtSigned(p.SG_CH_SHRUNK, 2)} — poor fit`,
    });
  }

  // --- season strokes gained by phase ---
  for (const ph of PHASES) {
    const q = f.pct[ph.key][p.id];
    if (q === undefined) continue; // no season stats for this player
    const v = p.form?.phases?.[ph.key as "ott" | "app" | "arg" | "putt"];
    if (v == null) continue;
    const r = f.rnk[ph.key][p.id];
    if (q >= 0.9) {
      out.push({
        severity: "good",
        text: `Strong ${ph.label} — top ${topPct(q)}% (${fmtSigned(v, 2)}, rank ${r})`,
      });
    } else if (q <= 0.1) {
      out.push({
        severity: "bad",
        text: `Weak ${ph.label} — bottom ${bottomPct(q)}% (${fmtSigned(v, 2)}, rank ${r})`,
      });
    }
  }

  // --- market ---
  if (p.LEVERAGE >= 3) {
    out.push({
      severity: "info",
      text: `Leverage ${fmtSigned(p.LEVERAGE, 1)} — model ahead of Vegas`,
    });
  } else if (p.LEVERAGE <= -3) {
    out.push({
      severity: "warn",
      text: `Leverage ${fmtSigned(p.LEVERAGE, 1)} — Vegas ahead of model`,
    });
  }

  // --- pricing ---
  const gap = rnk("P_TOP20") - rnk("SALARY");
  if (gap <= -18) {
    out.push({
      severity: "good",
      text: `Underpriced: P20 rank ${rnk("P_TOP20")} vs salary rank ${rnk("SALARY")}`,
    });
  } else if (gap >= 18) {
    out.push({
      severity: "bad",
      text: `Overpriced: P20 rank ${rnk("P_TOP20")} vs salary rank ${rnk("SALARY")}`,
    });
  }

  if (p.OWGR_RANK >= 150) {
    out.push({ severity: "warn", text: `Longshot: OWGR ${p.OWGR_RANK.toFixed(0)}` });
  }

  if (out.length === 0) {
    out.push({ severity: "none", text: "No outliers vs field on any tracked metric." });
  }
  return out;
}
