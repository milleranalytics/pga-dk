import type { Field, Player, Metric } from "./enrich";
import { fmtSigned } from "./format";

/**
 * The flag engine — an automatic scan for where a player stands out, either
 * against the CURRENT FIELD or against an absolute standard, so two players can
 * be compared without reading every number.
 *
 * Every threshold is in THRESHOLDS below. That is deliberate: the whole value
 * of this panel depends on knowing exactly what it fires on, so there are no
 * magic numbers buried in the rules.
 *
 * Provenance of each rule is marked:
 *   [S]  ported from the Streamlit app's player_flags()
 *   [D]  from the Claude Design handoff (DraftKings price/market angle)
 *
 * Two rules the design handoff is emphatic about, both honoured:
 *  - Every message embeds the actual VALUE and, where meaningful, the field
 *    RANK. The verdict alone is not the point; the number is.
 *  - Consistency: any two numbers describing the same quantity come from the
 *    same source, and any two describing DIFFERENT windows say so. That is why
 *    the two cut-related flags are named differently and both name their
 *    window — an early build showed "100% cuts made" above "CUTS /20 95%" and
 *    read as the app contradicting itself.
 */

export type Severity = "good" | "bad" | "warn" | "info" | "none";

export interface Flag {
  severity: Severity;
  text: string;
}

/** Every number the flag engine fires on, in one place. */
export const THRESHOLDS = {
  // --- form ---
  hotFormPct: 0.85, // [D] field-relative
  poorFormPct: 0.15, // [D]
  eliteFormAbs: 1.0, // [S] absolute strokes/round
  poorFormAbs: -0.5, // [S] absolute

  // --- cut streak, last-N starts ---
  coldStreak: 2, // [S] consecutive missed cuts
  steadyStreak: 6, // [S] consecutive made cuts

  // --- cut rate, 9-month window ---
  reliableCutPct: 95, // [D]
  volatileCutPct: 62, // [D]

  // --- ceiling, last 20 starts ---
  minStartsForCeiling: 8, // [S]
  highCeilingPct: 35, // [S] top-20 rate
  lowCeilingPct: 10, // [S]

  // --- sample size ---
  thinSampleRounds: 20, // [S] rounds in last 12 months

  // --- course, strokes-based ---
  courseFitSg: 0.8, // [D] SG_CH_SHRUNK
  poorCourseSg: -0.5, // [D]

  // --- course, finish-based ---
  minCourseEvents: 3, // [S] below this it is "thin history"
  courseHorseCutPct: 60, // [S]
  courseHorseBest: 15, // [S] best finish at or better than T15
  poorCourseCutPct: 40, // [S]

  // --- season SG by phase, field-relative ---
  strongPhasePct: 0.9, // [D]
  weakPhasePct: 0.1, // [D]

  // --- market / price ---
  leverageHigh: 3, // [D]
  leverageLow: -3, // [D]
  priceRankGap: 18, // [D] P20 rank vs salary rank
  valuePct: 0.88, // [D]
  poorValuePct: 0.12, // [D]
} as const;

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

/** Red first, then green, then cautions — the most actionable warning should
 *  not be buried under three positives. Same ordering Streamlit uses. */
const ORDER: Record<Severity, number> = { bad: 0, good: 1, warn: 2, info: 3, none: 4 };

export function playerFlags(p: Player, f: Field): Flag[] {
  const out: Flag[] = [];
  const pct = (m: Metric) => f.pct[m][p.id];
  const rnk = (m: Metric) => f.rnk[m][p.id];
  const N = f.players.length;
  const T = THRESHOLDS;

  // ---------------------------------------------------------------- form ---
  // Two form flags on purpose, and they say different things. The percentile
  // one answers "hot for THIS field"; the absolute one answers "hot, full
  // stop". In a weak field a +0.55 clears the first and not the second.
  if (pct("SG_FORM") >= T.hotFormPct) {
    out.push({
      severity: "good",
      text: `Hot form: SG ${fmtSigned(p.SG_FORM, 2)}/rd — top ${topPct(pct("SG_FORM"))}% of field`,
    });
  } else if (pct("SG_FORM") <= T.poorFormPct) {
    out.push({
      severity: "bad",
      text: `Poor form: SG ${fmtSigned(p.SG_FORM, 2)}/rd — rank ${rnk("SG_FORM")} of ${N}`,
    });
  }
  if (p.SG_FORM >= T.eliteFormAbs) {
    out.push({
      severity: "good",
      text: `Elite form: SG ${fmtSigned(p.SG_FORM, 2)}/rd — above +${T.eliteFormAbs.toFixed(1)} outright`,
    });
  } else if (p.SG_FORM <= T.poorFormAbs) {
    out.push({
      severity: "bad",
      text: `Losing strokes: SG ${fmtSigned(p.SG_FORM, 2)}/rd to the field`,
    });
  }

  // ------------------------------------------------------- cuts & streaks ---
  // "Steady" is the STREAK (Streamlit's meaning). The 9-month rate is
  // "Reliable" so the two can never be mistaken for each other, and both name
  // their window.
  const streak = p.form?.streak;
  if (streak) {
    const [run, kind] = streak;
    if (kind === "missed" && run >= T.coldStreak) {
      out.push({ severity: "bad", text: `Cold: ${run} straight missed cuts` });
    } else if (kind === "made" && run >= T.steadyStreak) {
      out.push({ severity: "good", text: `Steady: ${run} straight cuts made` });
    }
  }
  if (p.CUT_PERCENTAGE >= T.reliableCutPct) {
    out.push({
      severity: "good",
      text: `Reliable: ${p.CUT_PERCENTAGE.toFixed(0)}% cuts made (9mo)`,
    });
  } else if (p.CUT_PERCENTAGE <= T.volatileCutPct) {
    out.push({
      severity: "warn",
      text: `Volatile: ${p.CUT_PERCENTAGE.toFixed(0)}% cuts made (9mo)`,
    });
  }

  // ------------------------------------------------------------- ceiling ---
  // Top-20 rate is the model's actual target outcome, so this is the closest
  // thing to a direct historical read on the number being predicted.
  const starts = p.form?.results?.length ?? 0;
  const top20 = p.form?.top20_20;
  if (starts >= T.minStartsForCeiling && top20 != null) {
    if (top20 >= T.highCeilingPct) {
      out.push({ severity: "good", text: `High ceiling: ${top20.toFixed(0)}% top-20 in last 20` });
    } else if (top20 <= T.lowCeilingPct) {
      out.push({ severity: "warn", text: `Low ceiling: ${top20.toFixed(0)}% top-20 in last 20` });
    }
  }

  // --------------------------------------------------------- sample size ---
  // A caveat on every other form number for this player, so it is worth
  // firing even though it says nothing about quality.
  const r12 = p.form?.rounds_12m;
  if (r12 != null && r12 < T.thinSampleRounds) {
    out.push({ severity: "warn", text: `Thin sample: ${r12} rounds in last 12m` });
  }

  // -------------------------------------------------------------- course ---
  // Strokes and finishes are complementary reads, so both can fire. What must
  // NOT happen is two different "no history" messages, so the absence case is
  // resolved once, from the results table, before anything else is considered.
  const here = p.form?.course_here;
  const course = f.meta.course;
  if (!here) {
    out.push({ severity: "warn", text: `No starts at ${course}` });
  } else {
    if (here.ev < T.minCourseEvents) {
      out.push({
        severity: "warn",
        text: `Thin course history at ${course} (${here.ev} event${here.ev === 1 ? "" : "s"})`,
      });
    } else if (here.cut_pct >= T.courseHorseCutPct && here.best <= T.courseHorseBest) {
      out.push({
        severity: "good",
        text: `Course horse: ${course} — best T${here.best}, ${here.cut_pct}% cuts (${here.ev} events)`,
      });
    } else if (here.cut_pct < T.poorCourseCutPct) {
      out.push({
        severity: "bad",
        text: `Poor course fit: ${course} — ${here.cut_pct}% cuts (${here.ev} events)`,
      });
    }

    // SG_CH_SHRUNK is a 7-YEAR window while the events above are all-time, so
    // a player can have starts here and still have no measurement. Absence is
    // read from ch_window, never from the value being 0 — a genuinely
    // field-average player also rounds to 0 in the export.
    if (p.form?.ch_window === false) {
      out.push({
        severity: "warn",
        text: `No rounds at ${course} inside the model's 7-year window`,
      });
    } else if (p.SG_CH_SHRUNK >= T.courseFitSg) {
      out.push({
        severity: "good",
        text: `Course fit ${fmtSigned(p.SG_CH_SHRUNK, 2)} strokes/rd at ${course}`,
      });
    } else if (p.SG_CH_SHRUNK <= T.poorCourseSg) {
      out.push({
        severity: "bad",
        text: `Course history ${fmtSigned(p.SG_CH_SHRUNK, 2)} strokes/rd — poor fit`,
      });
    }
  }

  // ------------------------------------------ season strokes gained by phase ---
  for (const ph of PHASES) {
    const q = f.pct[ph.key][p.id];
    if (q === undefined) continue; // no season stats for this player
    const v = p.form?.phases?.[ph.key as "ott" | "app" | "arg" | "putt"];
    if (v == null) continue;
    const r = f.rnk[ph.key][p.id];
    if (q >= T.strongPhasePct) {
      out.push({
        severity: "good",
        text: `Strong ${ph.label} — top ${topPct(q)}% (${fmtSigned(v, 2)}, rank ${r})`,
      });
    } else if (q <= T.weakPhasePct) {
      out.push({
        severity: "bad",
        text: `Weak ${ph.label} — bottom ${bottomPct(q)}% (${fmtSigned(v, 2)}, rank ${r})`,
      });
    }
  }

  // ------------------------------------------------------ market & price ---
  if (pct("VAL") >= T.valuePct) {
    out.push({
      severity: "good",
      text: `Value play: ${p.VAL.toFixed(2)} P20%/$1k — rank ${rnk("VAL")} of ${N}`,
    });
  } else if (pct("VAL") <= T.poorValuePct) {
    out.push({
      severity: "bad",
      text: `Poor value: ${p.VAL.toFixed(2)} P20%/$1k — rank ${rnk("VAL")} of ${N}`,
    });
  }

  if (p.LEVERAGE >= T.leverageHigh) {
    out.push({
      severity: "info",
      text: `Leverage ${fmtSigned(p.LEVERAGE, 1)} — model ahead of Vegas`,
    });
  } else if (p.LEVERAGE <= T.leverageLow) {
    out.push({
      severity: "warn",
      text: `Leverage ${fmtSigned(p.LEVERAGE, 1)} — Vegas ahead of model`,
    });
  }

  const gap = rnk("P_TOP20") - rnk("SALARY");
  if (gap <= -T.priceRankGap) {
    out.push({
      severity: "good",
      text: `Underpriced: P20 rank ${rnk("P_TOP20")} vs salary rank ${rnk("SALARY")}`,
    });
  } else if (gap >= T.priceRankGap) {
    out.push({
      severity: "bad",
      text: `Overpriced: P20 rank ${rnk("P_TOP20")} vs salary rank ${rnk("SALARY")}`,
    });
  }

  if (out.length === 0) {
    out.push({ severity: "none", text: "No outliers vs field on any tracked metric." });
  }
  return out.sort((a, b) => ORDER[a.severity] - ORDER[b.severity]);
}
