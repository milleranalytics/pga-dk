import { c, font } from "../tokens";
import type { Player } from "../enrich";
import type { RoundPoint } from "../types";
import { fmtDate, fmtSigned, EM_DASH } from "../format";

/**
 * Player card sections (e) SG per round, (g) course history, (h) recent
 * results — everything that needs the history payload.
 *
 * Section (e) keeps the handoff's title, "SG PER ROUND". The handoff itself
 * says to retitle it to "per event" on the grounds that only score is stored
 * per event; that is wrong, and utils/features.build_rounds() derives real
 * per-ROUND SG from ROUNDS:1..4.
 */

// --- (e) SG per round -------------------------------------------------------

const VB_W = 416;
const VB_H = 152;
const X0 = 36;
const X1 = 410;
const Y_ZERO = 67;
const Y_SPAN = 51; // pixels for the full clamp
const CLAMP = 5.6; // strokes

export function SgScatter({ rounds }: { rounds: RoundPoint[] }) {
  if (!rounds.length) {
    return <div style={{ fontSize: 12, color: c.dim }}>No rounds in the last 24 months.</div>;
  }

  const times = rounds.map((r) => new Date(r[0] + "T00:00:00").getTime());
  const tMin = Math.min(...times);
  const tMax = Math.max(...times);
  const span = Math.max(tMax - tMin, 1);

  const x = (t: number) => X0 + ((t - tMin) / span) * (X1 - X0);
  const y = (v: number) => Y_ZERO - (Math.max(-CLAMP, Math.min(CLAMP, v)) / CLAMP) * Y_SPAN;

  // The trend is the third element of each point — an exponentially weighted
  // mean with the model's 100-day halflife, computed in Python. It is NOT
  // recomputed here: a JS reimplementation of pandas' time-based EWMA is
  // exactly the kind of port that quietly disagrees at the edges.
  const trendPts = rounds
    .map((r, i) => (r[2] == null ? null : `${x(times[i])},${y(r[2])}`))
    .filter(Boolean)
    .join(" ");

  const ticks = Array.from({ length: 5 }, (_, i) => {
    const t = tMin + (span * i) / 4;
    const d = new Date(t);
    return {
      x: x(t),
      label: `${d.toLocaleString("en-US", { month: "short" })} '${String(d.getFullYear()).slice(2)}`,
    };
  });

  return (
    <svg viewBox={`0 0 ${VB_W} ${VB_H}`} style={{ width: "100%", height: "auto" }}>
      <line x1={X0} y1={16} x2={X0} y2={118} stroke={c.line} strokeWidth={1} />
      <line
        x1={X0}
        y1={Y_ZERO}
        x2={X1}
        y2={Y_ZERO}
        stroke={c.axis}
        strokeWidth={1}
        strokeDasharray="3 3"
      />
      {[
        { v: 5, label: "+5" },
        { v: 0, label: "0" },
        { v: -5, label: "−5" },
      ].map((t) => (
        <text
          key={t.label}
          x={30}
          y={y(t.v) + 3}
          textAnchor="end"
          fill={c.dim}
          fontSize={9}
          fontFamily={font.mono}
        >
          {t.label}
        </text>
      ))}

      {rounds.map((r, i) => (
        <circle key={i} cx={x(times[i])} cy={y(r[1])} r={2.1} fill={c.scatter} opacity={0.75} />
      ))}

      {trendPts && (
        <polyline
          points={trendPts}
          fill="none"
          stroke={c.trend}
          strokeWidth={1.8}
          strokeLinejoin="round"
        />
      )}

      {ticks.map((t, i) => (
        <text
          key={i}
          x={t.x}
          y={134}
          textAnchor="middle"
          fill={c.dim}
          fontSize={9}
          fontFamily={font.mono}
        >
          {t.label}
        </text>
      ))}
    </svg>
  );
}

// --- (g) record at THIS week's course ---------------------------------------

/**
 * Scoped to this week's venue only, not a scrolling list of every course the
 * player has seen. This card is a this-week view: depth on the course actually
 * being played beats breadth across venues that are not. Other courses live in
 * the Course tab and the Results Browser.
 *
 * Note AVG FINISH counts a missed cut as 90 — the DB's fill value, and the same
 * convention Streamlit's course table uses. With few events one cut drags the
 * average hard, which is why every visit is listed underneath rather than left
 * hidden behind an aggregate.
 */
export function CourseHere({ p, thisCourse }: { p: Player; thisCourse: string }) {
  const here = p.form?.course_here;
  const events = p.form?.course_events ?? [];

  if (!here) {
    return (
      <div style={{ fontSize: 12, color: c.dim, lineHeight: 1.5 }}>
        No starts at {thisCourse}.{" "}
        <span style={{ color: c.dimmer }}>
          SG:C reads 0.00 because there is nothing to measure, not because it is neutral.
        </span>
      </div>
    );
  }

  return (
    <div>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(4, 1fr)",
          gap: 1,
          background: c.line,
          border: `1px solid ${c.line}`,
          borderRadius: 5,
          overflow: "hidden",
        }}
      >
        <Mini label="EVENTS" value={`${here.ev}`} />
        <Mini label="AVG FIN" value={here.avg.toFixed(1)} />
        <Mini
          label="BEST"
          value={`${here.best}`}
          color={here.best <= 10 ? c.green : here.best <= 20 ? c.text2 : c.text}
        />
        <Mini
          label="CUT%"
          value={`${here.cut_pct}`}
          color={here.cut_pct >= 80 ? c.green : here.cut_pct < 50 ? c.amber : c.text}
        />
      </div>

      {events.length > 0 && (
        <div style={{ marginTop: 10 }}>
          {events.map((r, i) => {
            const f = finishStyle(r.finish);
            return (
              <div
                key={i}
                style={{
                  display: "grid",
                  gridTemplateColumns: "74px 1fr 46px 50px",
                  height: 23,
                  alignItems: "center",
                  borderTop: `1px solid ${c.lineSoft}`,
                }}
              >
                <div style={{ fontFamily: font.mono, fontSize: 11.5, color: c.dim }}>
                  {fmtDate(r.date)}
                </div>
                <div
                  style={{
                    fontFamily: font.sans,
                    fontSize: 12,
                    color: c.text2,
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                    whiteSpace: "nowrap",
                    paddingRight: 6,
                  }}
                >
                  {r.tournament}
                </div>
                <div
                  style={{
                    fontFamily: font.mono,
                    fontSize: 11.5,
                    textAlign: "right",
                    paddingRight: 6,
                    color: r.sg == null ? c.dimmer : r.sg >= 0 ? c.greenSoft : c.redSoft,
                  }}
                >
                  {r.sg == null ? EM_DASH : fmtSigned(r.sg, 1)}
                </div>
                <div
                  style={{
                    fontFamily: font.mono,
                    fontSize: 11.5,
                    textAlign: "right",
                    color: f.color,
                    fontWeight: f.weight,
                  }}
                >
                  {f.text}
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

function Mini({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div style={{ background: c.surface, padding: "7px 9px" }}>
      <div style={{ fontFamily: font.mono, fontSize: 9, letterSpacing: "0.1em", color: c.dim }}>
        {label}
      </div>
      <div style={{ fontFamily: font.mono, fontSize: 16, fontWeight: 600, color: color ?? c.text }}>
        {value}
      </div>
    </div>
  );
}

// --- (h) recent results -----------------------------------------------------

const RES_TEMPLATE = "74px 1fr 46px 50px";

/** "T26" stays as-is, a win renders bare, CUT/W/D are their own state. */
function finishStyle(finish: string): { text: string; color: string; weight: number } {
  if (finish === "CUT" || finish === "W/D") {
    return { text: finish, color: c.dim, weight: 600 };
  }
  const n = parseInt(finish.replace(/^T/, ""), 10);
  const color = !Number.isFinite(n) ? c.muted : n <= 10 ? c.green : n <= 20 ? c.text2 : c.muted;
  return { text: finish, color, weight: 600 };
}

export function RecentResults({ p }: { p: Player }) {
  const rows = p.form?.results ?? [];
  if (!rows.length) {
    return <div style={{ fontSize: 12, color: c.dim }}>No results in the database.</div>;
  }
  return (
    <div style={{ maxHeight: 238, overflowY: "auto" }}>
      {rows.map((r, i) => {
        const f = finishStyle(r.finish);
        return (
          <div
            key={i}
            style={{
              display: "grid",
              gridTemplateColumns: RES_TEMPLATE,
              height: 24,
              alignItems: "center",
              borderTop: `1px solid ${c.lineSoft}`,
            }}
          >
            <div style={{ fontFamily: font.mono, fontSize: 11.5, color: c.dim }}>
              {fmtDate(r.date)}
            </div>
            <div
              style={{
                fontFamily: font.sans,
                fontSize: 12,
                color: c.text2,
                overflow: "hidden",
                textOverflow: "ellipsis",
                whiteSpace: "nowrap",
                paddingRight: 6,
              }}
            >
              {r.tournament}
            </div>
            <div
              style={{
                fontFamily: font.mono,
                fontSize: 11.5,
                textAlign: "right",
                paddingRight: 6,
                color: r.sg == null ? c.dimmer : r.sg >= 0 ? c.greenSoft : c.redSoft,
              }}
            >
              {r.sg == null ? EM_DASH : fmtSigned(r.sg, 1)}
            </div>
            <div
              style={{
                fontFamily: font.mono,
                fontSize: 11.5,
                textAlign: "right",
                color: f.color,
                fontWeight: f.weight,
              }}
            >
              {f.text}
            </div>
          </div>
        );
      })}
    </div>
  );
}
