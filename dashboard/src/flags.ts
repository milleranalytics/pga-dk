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

  // --- price: nothing ---
  // The handoff's VAL, LEVERAGE and P20-vs-salary-rank flags are all gone. Two
  // of them duplicated grid columns, and all three describe price efficiency,
  // which the optimizer acts on directly: an overpriced player is simply never
  // selected, and an underpriced one is targeted without being announced.
  // Flagging it spent attention on a decision already being made.
} as const;

/** One alias for the whole module — the rules below and the guide at the bottom
 *  must read from the SAME object, or the panel and its documentation drift. */
const T = THRESHOLDS;

/** "top 6% of field" — how far into the field this percentile sits. */
function topPct(p: number): number {
  return Math.max(1, Math.round((1 - p) * 100));
}

function bottomPct(p: number): number {
  return Math.max(1, Math.round(p * 100));
}

/** "3 events" / "1 event". Spelled out, never "ev" — abbreviated beside a column
 *  of probabilities and strokes-gained figures, "ev" reads as expected value. */
function evTxt(n: number): string {
  return `${n} event${n === 1 ? "" : "s"}`;
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
    ? `best T${here.best}, ${here.cut_pct}% cuts (${evTxt(here.ev)})`
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
        text: `Thin history at ${course}: ${evTxt(here.ev)}, ${sgTxt}`,
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

  if (out.length === 0) {
    out.push({ severity: "none", text: "No outliers vs field on any tracked metric." });
  }
  return out.sort((a, b) => ORDER[a.severity] - ORDER[b.severity]);
}

// --- what fires, and on what ------------------------------------------------

/**
 * The panel's own documentation, for the flyout on the Flags card.
 *
 * Every number in it is INTERPOLATED FROM `THRESHOLDS`, never retyped. That is
 * the only thing that makes a reference like this worth having: a hand-written
 * copy of the rules is correct on the day it is written and quietly wrong from
 * the first retune onwards — and these thresholds have already been retuned
 * three times (see the comments above). Retune a constant and this text moves
 * with it.
 *
 * The one thing NOT derivable is which side of a rule is field-relative and
 * which is absolute, so each trigger says so in words.
 */
export interface FlagRuleDoc {
  severity: Severity;
  name: string;
  /** The trigger, in the same terms the flag text uses. */
  when: string;
}

export interface FlagGroupDoc {
  title: string;
  /** Scope or caveat that applies to every rule in the group. */
  note?: string;
  rules: FlagRuleDoc[];
}

const topBand = (p: number) => `top ${Math.round((1 - p) * 100)}%`;
const bottomBand = (p: number) => `bottom ${Math.round(p * 100)}%`;

export const FLAG_GUIDE: FlagGroupDoc[] = [
  {
    title: "Form",
    note: "SG form — the model's recency-weighted strokes gained per round. Two rules per direction: one asks \"hot for THIS field\", the other \"hot, full stop\". In a weak field a +0.55 clears the first and not the second.",
    rules: [
      { severity: "good", name: "Hot form", when: `${topBand(T.hotFormPct)} of the field` },
      { severity: "bad", name: "Poor form", when: `${bottomBand(T.poorFormPct)} of the field` },
      {
        severity: "good",
        name: "Elite form",
        when: `above ${fmtSigned(T.eliteFormAbs, 2)}/rd outright`,
      },
      {
        severity: "bad",
        name: "Losing strokes",
        when: `at or below ${fmtSigned(T.poorFormAbs, 2)}/rd outright`,
      },
    ],
  },
  {
    title: "Cuts & streaks",
    note: "The streak is consecutive starts; the rate is a 9-month window. Both name their window because they routinely disagree.",
    rules: [
      { severity: "bad", name: "Cold", when: `${T.coldStreak}+ straight missed cuts` },
      { severity: "good", name: "Steady", when: `${T.steadyStreak}+ straight cuts made` },
      { severity: "good", name: "Reliable", when: `cut rate ${T.reliableCutPct}%+ (9mo)` },
      { severity: "warn", name: "Volatile", when: `cut rate ${T.volatileCutPct}% or worse (9mo)` },
    ],
  },
  {
    title: "Ceiling",
    note: `Top-20 rate over the last 20 starts — the model's actual target outcome. Silent below ${T.minStartsForCeiling} starts.`,
    rules: [
      { severity: "good", name: "High ceiling", when: `${T.highCeilingPct}%+ top-20 finishes` },
      { severity: "warn", name: "Low ceiling", when: `${T.lowCeilingPct}% or fewer` },
    ],
  },
  {
    title: "Sample size",
    note: "A caveat on every other form number for this player, not a verdict on him.",
    rules: [
      {
        severity: "warn",
        name: "Thin sample",
        when: `under ${T.thinSampleRounds} rounds in the last 12 months`,
      },
    ],
  },
  {
    title: "This week's course",
    note: "At most ONE of these fires. Strokes (SG:C, shrunk toward the field) and record (cuts and best finish) are weighed together, and the flag names both so it is clear which drove it.",
    rules: [
      {
        severity: "good",
        name: "Course horse",
        when: `SG:C in the ${topBand(T.courseFitPct)} of measured players, OR ${T.minCourseEvents}+ events with ${T.courseHorseCutPct}%+ cuts and a best of T${T.courseHorseBest} or better`,
      },
      {
        severity: "bad",
        name: "Struggles",
        when: `SG:C in the ${bottomBand(T.poorCoursePct)}, OR ${T.minCourseEvents}+ events with under ${T.poorCourseCutPct}% cuts`,
      },
      {
        severity: "warn",
        name: "Thin history",
        when: `fewer than ${T.minCourseEvents} events here, and neither rule above fired`,
      },
      { severity: "warn", name: "No starts", when: "never played this course" },
      {
        severity: "warn",
        name: "Pre-window only",
        when: "every start here predates the model's 7-year SG window, so SG:C is unmeasured rather than average",
      },
    ],
  },
  {
    title: "Season strokes gained",
    note: "PGA Tour season stats, ranked against the players in this field who HAVE them rather than against the whole field — the ones without season stats are left out of the ranking, not sorted to the bottom of it. Tee-to-green can fire alongside a phase flag: a player can be elite T2G on approach alone.",
    rules: [
      {
        severity: "good",
        name: "Strong phase",
        when: `${topBand(T.strongPhasePct)} in driving, approach, around-green or putting`,
      },
      { severity: "bad", name: "Weak phase", when: `${bottomBand(T.weakPhasePct)} in any of the four` },
      { severity: "good", name: "Elite ball-striker", when: `SG T2G in the ${topBand(T.strongTtgPct)}` },
      { severity: "bad", name: "Ball-striking cold", when: `SG T2G in the ${bottomBand(T.weakTtgPct)}` },
    ],
  },
];
