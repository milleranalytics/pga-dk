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
  // Recalibrated from the handoff's 62. A DK field's median cut rate is ~61%
  // (Rocket Classic 2026: 61.1), so 62 flagged half the field as an outlier —
  // it was describing the median player, not a tail. 40 is roughly the 15th
  // percentile and reads plainly: made fewer than two cuts in five.
  volatileCutPct: 40, // [D, retuned]

  // --- ceiling, last 20 starts ---
  minStartsForCeiling: 8, // [S]
  highCeilingPct: 35, // [S] top-20 rate
  // Also retuned, from Streamlit's 10. Its field is every active player;
  // a DK field's median top-20 rate is 15%, so 10 sat at the 25th percentile
  // and fired on 42%. At 5 the flag means "at most one top-20 in 20 starts".
  lowCeilingPct: 5, // [S, retuned]

  // --- sample size ---
  thinSampleRounds: 20, // [S] rounds in last 12 months

  // --- course, strokes-based, FIELD-RELATIVE among measured players ---
  // The handoff used absolute cutoffs (+0.8 / -0.5). Those do not travel
  // between venues: a course whose event draws a weaker field leaves most
  // returning players with positive SG there, so +0.8 landed at the 75th
  // percentile at Detroit and called a quarter of the field course horses.
  // Percentiles self-adjust to the venue and to who is actually in the field.
  courseFitPct: 0.85, // [D, retuned to a percentile]
  poorCoursePct: 0.15, // [D, retuned]

  // --- course, finish-based ---
  minCourseEvents: 3, // [S] below this it is "thin history"
  courseHorseCutPct: 60, // [S]
  courseHorseBest: 15, // [S] best finish at or better than T15
  poorCourseCutPct: 40, // [S]

  // --- season SG, field-relative ---
  strongPhasePct: 0.9, // [D]
  weakPhasePct: 0.1, // [D]
  strongTtgPct: 0.9, // [S] tee-to-green composite
  weakTtgPct: 0.1, // [S]

  // --- price ---
  // VAL and LEVERAGE thresholds are deliberately absent. Both are columns in
  // the grid and the optimizer already exploits value directly, so flagging
  // them spent attention on something already visible. The rank GAP survives
  // because comparing a player's P20 rank to his salary rank is the one price
  // read the table does not make for you.
  priceRankGap: 18, // [D] P20 rank vs salary rank
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
  // AT MOST ONE course flag. The previous version could fire three at once and
  // used "Course fit" for a strokes verdict beside "Poor course fit" for a
  // cut-rate verdict — same words, different metrics, reading as a
  // contradiction whenever they disagreed. Strokes and record are now
  // evaluated together and
  // reported in one sentence that names both, so it is always clear which
  // number drove the call. The At-this-course panel carries the full detail.
  const here = p.form?.course_here;
  const course = f.meta.course;
  const measured = p.form?.ch_window !== false;
  const sgTxt = measured ? `${fmtSigned(p.SG_CH_SHRUNK, 2)} str/rd` : "no SG";
  const recTxt = here
    ? `best T${here.best}, ${here.cut_pct}% cuts (${here.ev} ev)`
    : "";

  if (!here) {
    out.push({ severity: "warn", text: `No starts at ${course}` });
  } else if (!measured) {
    // Starts exist but all predate the model's 7-year window, so SG:C is 0 for
    // want of data rather than because the player is average.
    out.push({
      severity: "warn",
      text: `Only pre-window starts at ${course} — SG:C unmeasured (${recTxt})`,
    });
  } else {
    const chPct = f.pct.SG_CH_SHRUNK[p.id];
    const strongSg = chPct !== undefined && chPct >= T.courseFitPct;
    const weakSg = chPct !== undefined && chPct <= T.poorCoursePct;
    const enough = here.ev >= T.minCourseEvents;
    const strongRecord =
      enough && here.cut_pct >= T.courseHorseCutPct && here.best <= T.courseHorseBest;
    const weakRecord = enough && here.cut_pct < T.poorCourseCutPct;

    if (strongSg || strongRecord) {
      out.push({ severity: "good", text: `Course horse at ${course}: ${sgTxt}, ${recTxt}` });
    } else if (weakSg || weakRecord) {
      out.push({ severity: "bad", text: `Struggles at ${course}: ${sgTxt}, ${recTxt}` });
    } else if (!enough) {
      out.push({
        severity: "warn",
        text: `Thin history at ${course}: ${here.ev} event${here.ev === 1 ? "" : "s"}, ${sgTxt}`,
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

  // Tee-to-green is the ott+app+arg composite — the standard read on ball
  // striking, and the one stat most likely to be checked directly. It can
  // co-fire with an individual phase flag; that is informative rather than
  // redundant, since a player can be elite T2G on the back of approach alone.
  const ttg = p.form?.phases?.ttg;
  const ttgPct = f.pct.ttg[p.id];
  if (ttg != null && ttgPct !== undefined) {
    const r = f.rnk.ttg[p.id];
    if (ttgPct >= T.strongTtgPct) {
      out.push({
        severity: "good",
        text: `Elite ball-striker — top ${topPct(ttgPct)}% (SG T2G ${fmtSigned(ttg, 2)}, rank ${r})`,
      });
    } else if (ttgPct <= T.weakTtgPct) {
      out.push({
        severity: "bad",
        text: `Ball-striking cold — bottom ${bottomPct(ttgPct)}% (SG T2G ${fmtSigned(ttg, 2)}, rank ${r})`,
      });
    }
  }

  // ---------------------------------------------------------------- price ---
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
