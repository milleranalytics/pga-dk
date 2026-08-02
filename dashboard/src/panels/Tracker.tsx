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
                  : Math.abs(hitRate - avgPred) <= 0.02
                    ? c.green
                    : c.amber
              }
            />
          </StatCardRow>
          {pending > 0 && (
            <div style={{ marginTop: 8, fontSize: 11.5, color: c.dim }}>
              {pending} prediction{pending === 1 ? "" : "s"} awaiting results — excluded from
              every figure above.
            </div>
          )}
        </Section>

        <Section title="Hit rate by decile" sub="predicted vs realized top-20 rate">
          <DecileTable buckets={buckets} />
        </Section>

        <Section title="Calibration curve" sub="45° = perfectly calibrated" last>
          <CalibrationCurve buckets={buckets} />
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

const T = "78px 54px 76px 68px 1fr 62px";

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
        <div style={{ textAlign: "right" }}>DIFF</div>
      </div>
      {buckets.map((b) => {
        const pred = b.n ? b.predSum / b.n : null;
        const act = b.n ? b.hits / b.n : null;
        const diff = pred !== null && act !== null ? act - pred : null;
        // Thin buckets are noisy — under 20 observations a 10pt gap means
        // little, so the color stays neutral rather than crying miscalibration.
        const thin = b.n < 20;
        const diffColor =
          diff === null || thin
            ? c.dim
            : Math.abs(diff) <= 0.03
              ? c.green
              : Math.abs(diff) <= 0.08
                ? c.amber
                : c.red;
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
                  background: thin ? c.dimmer : c.dim,
                  borderRadius: 2,
                }}
              />
            </div>
            <div style={{ textAlign: "right", color: diffColor }}>
              {diff === null ? "—" : `${diff >= 0 ? "+" : "−"}${Math.abs(diff * 100).toFixed(1)}`}
            </div>
          </div>
        );
      })}
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
        <circle key={i} cx={x(p.px)} cy={y(p.py)} r={3} fill={c.green} />
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
