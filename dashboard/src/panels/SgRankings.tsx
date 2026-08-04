import { useMemo, useState } from "react";
import { c, font, dirColor } from "../tokens";
import type { SgRankRow } from "../types";
import type { Field } from "../enrich";
import { fmtSigned } from "../format";

/**
 * SG Rankings — recency-weighted strokes-gained form for EVERY active player,
 * not just this week's DK field. That scope is the point: it shows who is
 * playing well across the tour, including players not in this field.
 *
 * SG_FORM comes from sg_features_for_event() in Python, the same function that
 * builds the model's feature, so these numbers reconcile with the SG:F column
 * in the grid.
 */

const T = "48px minmax(150px,1fr) 66px 58px 52px 1fr";
const MIN_ROUNDS_DEFAULT = 16;

export default function SgRankings({
  rows,
  field,
  onSelect,
}: {
  rows: SgRankRow[];
  field: Field;
  onSelect: (id: string) => void;
}) {
  const [query, setQuery] = useState("");
  const [minRounds, setMinRounds] = useState(MIN_ROUNDS_DEFAULT);
  const [fieldOnly, setFieldOnly] = useState(false);

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    let r = rows.filter((x) => x.rounds_12m >= minRounds);
    if (fieldOnly) r = r.filter((x) => field.byId.has(x.player));
    if (q) r = r.filter((x) => x.player.toLowerCase().includes(q));
    return r;
  }, [rows, query, minRounds, fieldOnly, field.byId]);

  if (!rows.length) {
    return (
      <div style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center", color: c.dim, fontSize: 12.5 }}>
        No SG rankings in this slate.
      </div>
    );
  }

  return (
    <div style={{ flex: 1, display: "flex", flexDirection: "column", minHeight: 0 }}>
      <div
        style={{
          padding: "12px 16px",
          borderBottom: `1px solid ${c.line}`,
          display: "flex",
          alignItems: "center",
          gap: 14,
          flexWrap: "wrap",
        }}
      >
        <input
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Filter player…"
          style={{
            width: 160,
            background: c.surfaceAlt,
            border: `1px solid ${c.lineStrong}`,
            borderRadius: 4,
            padding: "6px 10px",
            fontSize: 12,
            color: c.text,
            outline: "none",
          }}
        />
        {/* Hides small-sample players whose SG_FORM rests on a handful of
            rounds — major-only LIV players, mostly. Slide to 0 for everyone. */}
        <label style={ctlLabel}>
          MIN RDS 12M
          <input
            type="range"
            min={0}
            max={60}
            value={minRounds}
            onChange={(e) => setMinRounds(Number(e.target.value))}
            style={{ width: 110, accentColor: c.blue }}
          />
          <span style={{ color: c.text2, width: 18 }}>{minRounds}</span>
        </label>
        <label style={{ ...ctlLabel, cursor: "pointer" }}>
          <input
            type="checkbox"
            checked={fieldOnly}
            onChange={(e) => setFieldOnly(e.target.checked)}
            style={{ accentColor: c.blue }}
          />
          THIS WEEK&apos;S FIELD ONLY
        </label>
        <span style={{ fontFamily: font.mono, fontSize: 10.5, color: c.dim, marginLeft: "auto" }}>
          {filtered.length} players · rank movement vs 30 days ago
        </span>
      </div>

      <div style={{ flex: 1, overflow: "auto", minHeight: 0 }}>
        <div
          style={{
            display: "grid",
            gridTemplateColumns: T,
            minWidth: 640,
            height: 30,
            alignItems: "center",
            background: c.surface,
            borderBottom: `1px solid ${c.lineStrong}`,
            position: "sticky",
            top: 0,
            zIndex: 2,
            fontFamily: font.mono,
            fontSize: 10,
            letterSpacing: "0.09em",
            color: c.muted,
          }}
        >
          <div style={{ paddingLeft: 10 }}>#</div>
          <div style={{ paddingLeft: 10 }}>PLAYER</div>
          <div style={{ textAlign: "right", paddingRight: 10 }}>SG FORM</div>
          <div style={{ textAlign: "right", paddingRight: 10 }}>TREND</div>
          <div style={{ textAlign: "right", paddingRight: 10 }}>RDS</div>
          <div style={{ paddingLeft: 10 }}>LAST 20 ROUNDS</div>
        </div>

        <div style={{ minWidth: 640 }}>
          {filtered.map((r) => {
            const inField = field.byId.has(r.player);
            return (
              <div
                key={r.player}
                onClick={() => inField && onSelect(r.player)}
                style={{
                  display: "grid",
                  gridTemplateColumns: T,
                  alignItems: "center",
                  height: 30,
                  borderBottom: `1px solid ${c.lineSoft}`,
                  fontFamily: font.mono,
                  fontSize: 11.5,
                  cursor: inField ? "pointer" : "default",
                  // A quiet tint marks who is actually playable this week.
                  background: inField ? "rgba(87,217,138,0.045)" : undefined,
                }}
              >
                <div style={{ paddingLeft: 10, color: c.dim }}>{r.rank}</div>
                <div
                  style={{
                    fontFamily: font.sans,
                    fontSize: 12.5,
                    paddingLeft: 10,
                    color: inField ? c.text : c.muted,
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                    whiteSpace: "nowrap",
                  }}
                >
                  {r.player}
                </div>
                <div
                  style={{
                    textAlign: "right",
                    paddingRight: 10,
                    fontWeight: 500,
                    color: dirColor(r.sg_form),
                  }}
                >
                  {fmtSigned(r.sg_form, 2)}
                </div>
                <Trend move={r.move} />
                <div style={{ textAlign: "right", paddingRight: 10, color: c.muted }}>
                  {r.rounds_12m}
                </div>
                <div style={{ paddingLeft: 10, paddingRight: 12 }}>
                  <Spark values={r.spark} />
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

function Trend({ move }: { move: number | null }) {
  if (move === null) {
    return <div style={{ textAlign: "right", paddingRight: 10, color: c.dimmer }}>NEW</div>;
  }
  const color = move === 0 ? c.dim : dirColor(move);
  const text = move === 0 ? "—" : `${move > 0 ? "+" : "−"}${Math.abs(move)}`;
  return <div style={{ textAlign: "right", paddingRight: 10, color }}>{text}</div>;
}

/** Fixed ±6 scale so sparklines are comparable ACROSS rows — auto-scaling each
 *  row independently would make a flat player look as volatile as a wild one. */
const SPARK_W = 150;
const SPARK_H = 18;
const SPARK_CLAMP = 6;

function Spark({ values }: { values: number[] }) {
  if (!values.length) return null;
  const x = (i: number) => (values.length === 1 ? 0 : (i / (values.length - 1)) * SPARK_W);
  const y = (v: number) =>
    SPARK_H / 2 - (Math.max(-SPARK_CLAMP, Math.min(SPARK_CLAMP, v)) / SPARK_CLAMP) * (SPARK_H / 2);
  return (
    <svg viewBox={`0 0 ${SPARK_W} ${SPARK_H}`} style={{ width: "100%", maxWidth: SPARK_W, height: SPARK_H }}>
      <line x1={0} y1={SPARK_H / 2} x2={SPARK_W} y2={SPARK_H / 2} stroke={c.axis} strokeWidth={0.7} />
      <polyline
        points={values.map((v, i) => `${x(i)},${y(v)}`).join(" ")}
        fill="none"
        stroke={c.trend}
        strokeWidth={1.2}
        strokeLinejoin="round"
      />
    </svg>
  );
}

const ctlLabel: React.CSSProperties = {
  display: "flex",
  alignItems: "center",
  gap: 7,
  fontFamily: font.mono,
  fontSize: 9.5,
  letterSpacing: "0.1em",
  color: c.dim,
};
