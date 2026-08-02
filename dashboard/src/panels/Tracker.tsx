import { useMemo, useState } from "react";
import { c, font } from "../tokens";
import type { TrackerRow, WeekRow } from "../types";
import { Section, StatCard, StatCardRow } from "../components/primitives";
import { fmtDate } from "../format";

/**
 * Prediction Tracker — is the model's stated probability believable?
 *
 * A calibrated P(top-20) of 0.30 should hit ~30% of the time. The decile table
 * answers that directly; the curve is support. The handoff is explicit that
 * numbers are read faster than charts here, so the table leads.
 *
 * Source is slate.tracker — the logged predictions table joined to actual
 * finishes on (TOURNAMENT, ENDING_DATE, PLAYER), computed in Python. Rows
 * whose event has not been played yet carry finish === null and are excluded
 * from every calibration figure but still counted as pending.
 */

const DECILES = 10;

interface Bucket {
  lo: number;
  hi: number;
  n: number;
  predSum: number;
  hits: number;
}

/** Forward-chained eval baseline from experiments/forward_eval.py. The number
 *  every week is judged against. */
const BASELINE_HITS = 6.49;

export default function Tracker({ rows, weeks }: { rows: TrackerRow[]; weeks: WeekRow[] }) {
  const [week, setWeek] = useState<string>("all");

  // Options for the calibration scope dropdown — distinct from the `weeks`
  // prop, which is the per-week track record.
  const weekOptions = useMemo(() => {
    const m = new Map<string, string>();
    for (const r of rows) m.set(r.date, r.tournament);
    return [...m.entries()].sort((a, b) => b[0].localeCompare(a[0]));
  }, [rows]);

  const scoped = useMemo(
    () => (week === "all" ? rows : rows.filter((r) => r.date === week)),
    [rows, week],
  );

  const graded = useMemo(
    () => scoped.filter((r) => r.finish !== null && r.p_top20 !== null),
    [scoped],
  );
  const pending = scoped.length - graded.length;

  const { buckets, brier, hitRate, avgPred } = useMemo(() => {
    const bs: Bucket[] = Array.from({ length: DECILES }, (_, i) => ({
      lo: i / DECILES,
      hi: (i + 1) / DECILES,
      n: 0,
      predSum: 0,
      hits: 0,
    }));
    let se = 0;
    let hits = 0;
    let predSum = 0;
    for (const r of graded) {
      const p = r.p_top20 as number;
      const hit = r.hit ? 1 : 0;
      // Math.min guards p === 1 landing in a non-existent 11th bucket.
      const i = Math.min(DECILES - 1, Math.floor(p * DECILES));
      bs[i].n++;
      bs[i].predSum += p;
      bs[i].hits += hit;
      se += (p - hit) ** 2;
      hits += hit;
      predSum += p;
    }
    return {
      buckets: bs,
      brier: graded.length ? se / graded.length : null,
      hitRate: graded.length ? hits / graded.length : null,
      avgPred: graded.length ? predSum / graded.length : null,
    };
  }, [graded]);

  if (rows.length === 0) {
    return (
      <Empty text="No predictions logged yet — the notebook's Export cell starts the log." />
    );
  }

  const gradedWeeks = weeks.filter((w) => w.graded && w.hits !== null);
  const avgHits = gradedWeeks.length
    ? gradedWeeks.reduce((a, w) => a + (w.hits as number), 0) / gradedWeeks.length
    : null;

  return (
    <div style={{ flex: 1, overflowY: "auto", padding: "0 0 30px" }}>
      <div style={{ maxWidth: 880 }}>
        {/* Leads the tab: this is the week-to-week health check, and the same
            figure the notebook's Report Card cell prints. */}
        <Section
          title="Weekly track record"
          sub={
            avgHits === null
              ? "no graded weeks yet"
              : `${gradedWeeks.length} graded · avg ${avgHits.toFixed(2)} vs ${BASELINE_HITS} baseline`
          }
        >
          <WeeksTable weeks={weeks} />
        </Section>

        <Section
          title="Calibration"
          sub={
            <select
              value={week}
              onChange={(e) => setWeek(e.target.value)}
              style={{
                background: c.surfaceAlt,
                color: c.text,
                border: `1px solid ${c.lineStrong}`,
                borderRadius: 4,
                fontSize: 11,
                fontFamily: font.mono,
                padding: "3px 6px",
              }}
            >
              <option value="all">all weeks</option>
              {weekOptions.map(([d, t]) => (
                <option key={d} value={d}>
                  {fmtDate(d)} · {t}
                </option>
              ))}
            </select>
          }
        >
          <StatCardRow>
            <StatCard label="GRADED" value={`${graded.length}`} />
            <StatCard
              label="BRIER"
              value={brier === null ? "—" : brier.toFixed(4)}
              color={c.blue}
            />
            <StatCard
              label="PREDICTED"
              value={avgPred === null ? "—" : `${(avgPred * 100).toFixed(1)}%`}
            />
            {/* Predicted vs actual side by side is the headline: if the model
                is calibrated these two match. */}
            <StatCard
              label="ACTUAL"
              value={hitRate === null ? "—" : `${(hitRate * 100).toFixed(1)}%`}
              color={
                hitRate === null || avgPred === null
                  ? c.text
                  : Math.abs(hitRate - avgPred) < 2 * nullSE(avgPred, graded.length)
                    ? c.green
                    : c.amber
              }
            />
          </StatCardRow>

          {/* A Brier score is meaningless in isolation — it depends entirely on
              the base rate. The reference is the score you would get by
              ignoring the model and predicting the base rate for everyone;
              beating it is the definition of the model having any skill. */}
          {brier !== null && hitRate !== null && (
            <div style={{ marginTop: 9, fontSize: 11.5, color: c.dim, lineHeight: 1.55 }}>
              Brier <span style={{ color: c.text2 }}>{brier.toFixed(4)}</span> vs{" "}
              <span style={{ color: c.text2 }}>{(hitRate * (1 - hitRate)).toFixed(4)}</span> for
              predicting the {(hitRate * 100).toFixed(1)}% base rate for every player —{" "}
              <span style={{ color: c.green }}>
                {((1 - brier / Math.max(hitRate * (1 - hitRate), 1e-9)) * 100).toFixed(0)}% better
              </span>
              . Predicted vs actual differ by{" "}
              {(Math.abs((hitRate - (avgPred ?? 0)) / nullSE(avgPred ?? 0, graded.length))).toFixed(
                1,
              )}{" "}
              standard errors.
            </div>
          )}

          {pending > 0 && (
            <div style={{ marginTop: 6, fontSize: 11.5, color: c.dim }}>
              {pending} prediction{pending === 1 ? "" : "s"} awaiting results — excluded from
              every figure above.
            </div>
          )}
        </Section>

        <Section title="Hit rate by decile" sub="predicted vs realized top-20 rate">
          <DecileTable buckets={buckets} />
        </Section>

        <Section title="Calibration curve" sub="45° = perfectly calibrated · point size = sample" last>
          <CalibrationCurve buckets={buckets} />
          <div style={{ marginTop: 6, fontSize: 11, color: c.dim, lineHeight: 1.5, maxWidth: 420 }}>
            Above the line, the model is under-forecasting that band; below it, over-forecasting.
            Only the large points carry weight — the small ones move a long way on one result.
          </div>
        </Section>
      </div>
    </div>
  );
}

const WT = "82px minmax(150px,1fr) 60px 70px 74px 62px";

function WeeksTable({ weeks }: { weeks: WeekRow[] }) {
  if (!weeks.length) {
    return <div style={{ fontSize: 12, color: c.dim }}>No weeks logged yet.</div>;
  }
  return (
    <div>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: WT,
          gap: "0 8px",
          fontFamily: font.mono,
          fontSize: 9,
          letterSpacing: "0.06em",
          color: c.dim,
          paddingBottom: 4,
        }}
      >
        <div>WEEK</div>
        <div>TOURNAMENT</div>
        <div style={{ textAlign: "right" }}>FIELD</div>
        <div style={{ textAlign: "right" }}>EXPECTED</div>
        <div style={{ textAlign: "right" }}>TOP-15 HITS</div>
        <div style={{ textAlign: "right" }}>CUT%</div>
      </div>
      {weeks.map((w) => (
        <div
          key={w.date + w.tournament}
          style={{
            display: "grid",
            gridTemplateColumns: WT,
            gap: "0 8px",
            height: 26,
            alignItems: "center",
            borderTop: `1px solid ${c.lineSoft}`,
            fontFamily: font.mono,
            fontSize: 11.5,
          }}
        >
          <div style={{ color: c.dim }}>{fmtDate(w.date)}</div>
          <div
            style={{
              fontFamily: font.sans,
              fontSize: 12,
              color: c.text2,
              overflow: "hidden",
              textOverflow: "ellipsis",
              whiteSpace: "nowrap",
            }}
          >
            {w.tournament}
          </div>
          <div style={{ textAlign: "right", color: c.muted }}>{w.players}</div>
          {/* Expected is the summed P(top-20) of the model's top 15 — what it
              PREDICTED it would hit. Hits is what actually happened. */}
          <div style={{ textAlign: "right", color: c.muted }}>{w.expected.toFixed(1)}</div>
          <div
            style={{
              textAlign: "right",
              fontWeight: 600,
              color:
                w.hits === null
                  ? c.dimmer
                  : w.hits >= w.expected
                    ? c.green
                    : w.hits >= w.expected - 2
                      ? c.text
                      : c.amber,
            }}
          >
            {w.hits === null ? "pending" : w.hits}
          </div>
          <div style={{ textAlign: "right", color: w.cut_rate === null ? c.dimmer : c.muted }}>
            {w.cut_rate === null ? "—" : `${w.cut_rate}%`}
          </div>
        </div>
      ))}
    </div>
  );
}

/**
 * Standard error of the realized rate under the null hypothesis "the model is
 * calibrated here", i.e. assuming the true rate equals the PREDICTED rate.
 *
 * Using the predicted rate rather than the observed one matters. With the
 * observed rate, a bucket holding one prediction that hit gives p=1, variance
 * 0, and an infinitely significant result — the table would scream about its
 * thinnest bucket. Under the null the variance comes from the model's own
 * claim, which is well defined at any sample size.
 */
function nullSE(pPred: number, n: number): number {
  if (n <= 0) return Infinity;
  return Math.sqrt(Math.max(pPred * (1 - pPred), 1e-9) / n);
}

/** Grey below 2 SE: at that point the gap is indistinguishable from sampling
 *  noise and colouring it would invent a finding. */
function diffColor(diff: number, se: number): string {
  const z = Math.abs(diff) / se;
  if (z < 2) return c.dim;
  if (z < 3) return c.amber;
  return c.red;
}

const T = "76px 46px 70px 64px 1fr 132px";

function DecileTable({ buckets }: { buckets: Bucket[] }) {
  const maxN = Math.max(...buckets.map((b) => b.n), 1);
  return (
    <div>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: T,
          gap: "0 8px",
          fontFamily: font.mono,
          fontSize: 9,
          letterSpacing: "0.06em",
          color: c.dim,
          paddingBottom: 4,
        }}
      >
        <div>DECILE</div>
        <div style={{ textAlign: "right" }}>N</div>
        <div style={{ textAlign: "right" }}>PREDICTED</div>
        <div style={{ textAlign: "right" }}>ACTUAL</div>
        <div>SAMPLE</div>
        <div style={{ textAlign: "right" }}>DIFF ± 95%</div>
      </div>
      {buckets.map((b) => {
        const pred = b.n ? b.predSum / b.n : null;
        const act = b.n ? b.hits / b.n : null;
        const diff = pred !== null && act !== null ? act - pred : null;
        const se = pred !== null ? nullSE(pred, b.n) : Infinity;
        const band = 2 * se; // ~95%
        const color = diff === null ? c.dim : diffColor(diff, se);
        return (
          <div
            key={b.lo}
            style={{
              display: "grid",
              gridTemplateColumns: T,
              gap: "0 8px",
              height: 24,
              alignItems: "center",
              borderTop: `1px solid ${c.lineSoft}`,
              fontFamily: font.mono,
              fontSize: 11.5,
            }}
          >
            <div style={{ color: c.muted }}>
              {(b.lo * 100).toFixed(0)}–{(b.hi * 100).toFixed(0)}%
            </div>
            <div style={{ textAlign: "right", color: b.n ? c.text2 : c.dimmer }}>{b.n}</div>
            <div style={{ textAlign: "right", color: c.text2 }}>
              {pred === null ? "—" : `${(pred * 100).toFixed(1)}%`}
            </div>
            <div style={{ textAlign: "right", color: act === null ? c.dimmer : c.text }}>
              {act === null ? "—" : `${(act * 100).toFixed(1)}%`}
            </div>
            <div style={{ height: 5, background: c.surface, borderRadius: 2 }}>
              <div
                style={{
                  height: "100%",
                  width: `${(b.n / maxN) * 100}%`,
                  background: c.dim,
                  borderRadius: 2,
                }}
              />
            </div>
            {/* The band is the point: a -28.8 on two predictions carries a
                +/-70 uncertainty and says nothing at all. */}
            <div style={{ textAlign: "right" }}>
              {diff === null ? (
                <span style={{ color: c.dimmer }}>—</span>
              ) : (
                <>
                  <span style={{ color }}>
                    {diff >= 0 ? "+" : "−"}
                    {Math.abs(diff * 100).toFixed(1)}
                  </span>
                  <span style={{ color: c.dimmer }}> ±{(band * 100).toFixed(1)}</span>
                </>
              )}
            </div>
          </div>
        );
      })}
      <div style={{ marginTop: 8, fontSize: 11, color: c.dim, lineHeight: 1.5 }}>
        ± is the 95% band for the gap if the model were perfectly calibrated in that
        bucket. A grey difference is smaller than the band — indistinguishable from
        sampling noise, not evidence of anything. Amber and red mark gaps that survive it.
      </div>
    </div>
  );
}

function CalibrationCurve({ buckets }: { buckets: Bucket[] }) {
  const S = 260;
  const PAD = 26;
  const x = (v: number) => PAD + v * (S - PAD - 8);
  const y = (v: number) => S - PAD - v * (S - PAD - 8);

  const pts = buckets
    .filter((b) => b.n > 0)
    .map((b) => ({ px: b.predSum / b.n, py: b.hits / b.n, n: b.n }));
  // Radius by sample size, so the eye weights the points the way the evidence
  // does. Without it a bucket of 1 draws as loudly as a bucket of 160 and the
  // curve looks wild when only its thinnest end is moving.
  const maxN = Math.max(...pts.map((p) => p.n), 1);
  const r = (n: number) => 2 + 4 * Math.sqrt(n / maxN);

  return (
    <svg viewBox={`0 0 ${S} ${S}`} style={{ width: S, height: S, maxWidth: "100%" }}>
      <line x1={x(0)} y1={y(0)} x2={x(0)} y2={y(1)} stroke={c.line} />
      <line x1={x(0)} y1={y(0)} x2={x(1)} y2={y(0)} stroke={c.line} />
      <line
        x1={x(0)}
        y1={y(0)}
        x2={x(1)}
        y2={y(1)}
        stroke={c.axis}
        strokeDasharray="3 3"
      />
      {[0, 0.5, 1].map((v) => (
        <g key={v}>
          <text
            x={x(v)}
            y={S - PAD + 12}
            textAnchor="middle"
            fill={c.dim}
            fontSize={9}
            fontFamily={font.mono}
          >
            {(v * 100).toFixed(0)}
          </text>
          <text
            x={PAD - 6}
            y={y(v) + 3}
            textAnchor="end"
            fill={c.dim}
            fontSize={9}
            fontFamily={font.mono}
          >
            {(v * 100).toFixed(0)}
          </text>
        </g>
      ))}
      <polyline
        points={pts.map((p) => `${x(p.px)},${y(p.py)}`).join(" ")}
        fill="none"
        stroke={c.trend}
        strokeWidth={1.8}
        strokeLinejoin="round"
      />
      {pts.map((p, i) => (
        <circle key={i} cx={x(p.px)} cy={y(p.py)} r={r(p.n)} fill={c.green}>
          <title>{`${(p.px * 100).toFixed(1)}% predicted → ${(p.py * 100).toFixed(1)}% actual (n=${p.n})`}</title>
        </circle>
      ))}
    </svg>
  );
}

function Empty({ text }: { text: string }) {
  return (
    <div style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center", padding: 40 }}>
      <div style={{ color: c.dim, fontSize: 12.5, textAlign: "center", maxWidth: 420 }}>{text}</div>
    </div>
  );
}
