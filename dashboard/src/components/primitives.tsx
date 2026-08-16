import type { CSSProperties, ReactNode } from "react";
import { c, font, rankColor } from "../tokens";
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

/**
 * A section with its uppercase heading and optional right sub-label.
 *
 * Two variants. The default is the original flush block separated by a hairline
 * rule — still what the Tracker uses, where sections are full-width and few.
 * `card` wraps the section in its own panel with a gap around it, for the
 * player card: eight stacked sections in a 448px column need a stronger break
 * than a 1px line at 14px spacing, or the eye reads them as one long list.
 */
export function Section({
  title,
  sub,
  last,
  card,
  children,
}: {
  title: string;
  sub?: ReactNode;
  last?: boolean;
  card?: boolean;
  children: ReactNode;
}) {
  return (
    <div
      style={
        card
          ? {
              background: c.panel,
              border: `1px solid ${c.line}`,
              borderRadius: 6,
              padding: "12px 14px 14px",
            }
          : {
              padding: last ? "14px 16px 20px" : "14px 16px",
              borderBottom: last ? undefined : `1px solid ${c.line}`,
            }
      }
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

/** `warn` keeps amber — a flag IS a caution about the player, which is the one
 *  place the third hue is doing its stated job. `info` drops blue: blue means
 *  lineup membership now, and a dot that colour would claim this player is in
 *  the build. (No flag currently emits `info`; the mapping exists so one added
 *  later cannot silently reintroduce it.) */
const SEVERITY_COLOR: Record<Severity, string> = {
  good: c.green,
  bad: c.red,
  warn: c.amber,
  info: c.text2,
  none: c.dim,
};

/** Exported so the flags reference panel draws its dots from the same map the
 *  live flags use. A legend that mixes its own colours is worse than none. */
export function severityColor(s: Severity): string {
  return SEVERITY_COLOR[s];
}

/** The dot. Shared by a live flag row and by the reference panel's legend, so
 *  the two cannot end up different sizes or different shades of the same idea. */
export function SeverityDot({ severity }: { severity: Severity }) {
  return (
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
  );
}

export function FlagRow({ severity, text }: { severity: Severity; text: string }) {
  return (
    <div style={{ display: "flex", gap: 9, alignItems: "flex-start" }}>
      <SeverityDot severity={severity} />
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
  neutral = false,
}: {
  label: string;
  value: string;
  pct: number | undefined;
  rank: number | undefined;
  /** Suppress the good/bad colouring for metrics that only have an ordering,
   *  not a direction — a high rank is not automatically a green one. Such a bar
   *  is grey: it was blue, which looked like it encoded something and did not.
   *  The bar's LENGTH is the whole message. */
  neutral?: boolean;
}) {
  // No percentile means the metric was never measured for this player — a
  // different state from "measured, and worst in the field". An empty bar at
  // full width would read as the latter, so the row is dimmed instead.
  const unmeasured = pct === undefined;
  const fill = unmeasured ? 0 : pct;
  // The SAME ramp every ranked number in the app takes — green for the top
  // tenth of the field, red for the bottom fifth, four greys in between. This
  // panel is where the ramp is most literally true: the bar's LENGTH is the
  // percentile and the bar's COLOUR is the band that percentile falls in, so
  // the two can never disagree. A row here and its column in the grid are now
  // the same colour by construction, because they read the same pct.
  const color = neutral ? c.dim : rankColor(pct);
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
