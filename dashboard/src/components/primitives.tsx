import type { CSSProperties, ReactNode } from "react";
import { c, font } from "../tokens";
import type { Severity } from "../flags";

/**
 * The primitives layer. Every screen composes from these rather than
 * re-inventing a heading or a bar, which is what keeps the Prediction Tracker
 * and Results Browser consistent without redesigning them.
 *
 * Rules these encode, in priority order:
 *  1. Every number is Mono; every micro-label is uppercase Mono 9-10px with
 *     letter-spacing. Only names, headings and sentences are Sans.
 *  2. No new accent colors. Green = good/model, red = bad, amber = caution,
 *     blue = market/selection.
 *  3. Context, not bare values — anywhere a number appears, show its rank or
 *     percentile against the relevant population.
 *  4. No shadows, no motion.
 */

/** A card section with its uppercase heading and optional right sub-label. */
export function Section({
  title,
  sub,
  last,
  children,
}: {
  title: string;
  sub?: ReactNode;
  last?: boolean;
  children: ReactNode;
}) {
  return (
    <div
      style={{
        padding: last ? "14px 16px 20px" : "14px 16px",
        borderBottom: last ? undefined : `1px solid ${c.line}`,
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "baseline",
          justifyContent: "space-between",
          marginBottom: 10,
        }}
      >
        <div style={microLabel(0.14)}>{title}</div>
        {sub != null && (
          <div style={{ fontFamily: font.mono, fontSize: 10, color: c.dim }}>{sub}</div>
        )}
      </div>
      {children}
    </div>
  );
}

export function microLabel(spacing = 0.1): CSSProperties {
  return {
    fontFamily: font.mono,
    fontSize: 10,
    fontWeight: 600,
    letterSpacing: `${spacing}em`,
    color: c.muted,
    textTransform: "uppercase",
  };
}

/** The 4-up stat row. The 1px gap over a line-colored background is what draws
 *  the dividers — there are no borders between cards. */
export function StatCardRow({ children }: { children: ReactNode }) {
  return (
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
      {children}
    </div>
  );
}

export function StatCard({
  label,
  value,
  color,
}: {
  label: string;
  value: string;
  color?: string;
}) {
  return (
    <div style={{ background: c.surface, padding: "8px 10px" }}>
      <div
        style={{
          fontFamily: font.mono,
          fontSize: 9,
          letterSpacing: "0.1em",
          color: c.dim,
        }}
      >
        {label}
      </div>
      {/* Never two values in one card: at 19px mono a combined string wraps
          and inflates all four cards. */}
      <div
        style={{
          fontFamily: font.mono,
          fontSize: 19,
          fontWeight: 600,
          color: color ?? c.text,
          whiteSpace: "nowrap",
        }}
      >
        {value}
      </div>
    </div>
  );
}

const SEVERITY_COLOR: Record<Severity, string> = {
  good: c.green,
  bad: c.red,
  warn: c.amber,
  info: c.blue,
  none: c.dim,
};

export function FlagRow({ severity, text }: { severity: Severity; text: string }) {
  return (
    <div style={{ display: "flex", gap: 9, alignItems: "flex-start" }}>
      <div
        style={{
          width: 7,
          height: 7,
          borderRadius: "50%",
          background: SEVERITY_COLOR[severity],
          marginTop: 5,
          flex: "none",
        }}
      />
      <div style={{ fontSize: 12.5, color: c.text2, lineHeight: 1.35 }}>{text}</div>
    </div>
  );
}

/** label · value · percentile bar · field rank. */
export function PercentileBar({
  label,
  value,
  pct,
  rank,
}: {
  label: string;
  value: string;
  pct: number | undefined;
  rank: number | undefined;
}) {
  // No percentile means the metric was never measured for this player — a
  // different state from "measured, and worst in the field". An empty bar at
  // full width would read as the latter, so the row is dimmed instead.
  const unmeasured = pct === undefined;
  const fill = unmeasured ? 0 : pct;
  const color = fill >= 0.8 ? c.green : fill >= 0.45 ? c.dim : c.dimmer;
  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "96px 58px 1fr 44px",
        gap: 8,
        height: 20,
        alignItems: "center",
      }}
    >
      <div style={{ fontFamily: font.mono, fontSize: 11, color: c.muted }}>{label}</div>
      <div
        style={{
          fontFamily: font.mono,
          fontSize: 12,
          color: unmeasured ? c.dimmer : c.text,
          textAlign: "right",
        }}
      >
        {value}
      </div>
      <div style={{ height: 5, background: c.surface, borderRadius: 2 }}>
        {!unmeasured && (
          <div
            style={{
              height: "100%",
              width: `${fill * 100}%`,
              background: color,
              borderRadius: 2,
            }}
          />
        )}
      </div>
      <div style={{ fontFamily: font.mono, fontSize: 10, color: c.dimmer }}>
        {unmeasured ? "n/a" : `#${rank}`}
      </div>
    </div>
  );
}
