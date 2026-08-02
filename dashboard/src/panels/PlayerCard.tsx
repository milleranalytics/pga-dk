import { c, font } from "../tokens";
import type { Field, Player, Metric } from "../enrich";
import { playerFlags } from "../flags";
import { Section, StatCard, StatCardRow, FlagRow, PercentileBar } from "../components/primitives";
import { fmtSalary, fmtSigned, fmtOdds, EM_DASH } from "../format";
import { SgScatter, CourseHistory, RecentResults } from "./CardHistory";

/**
 * The player card — sections (a) header/stats, (b) flags, (c) form profile,
 * (d) SG by phase, (f) percentile vs field.
 *
 * Sections (e) SG per round, (g) course history and (h) recent results need the
 * history payload and land in Phase 2. They are omitted rather than stubbed
 * with placeholder data: the handoff's consistency rule exists because an early
 * prototype showed a real figure next to a synthesized one and read as the app
 * contradicting itself.
 */

export interface PlayerCardProps {
  field: Field;
  player: Player | null;
  inLineup: boolean;
  locked: boolean;
  excluded: boolean;
  onTogglePick: (id: string) => void;
  onToggleLock: (id: string) => void;
  onToggleExclude: (id: string) => void;
}

const PHASE_ROWS: { key: Metric; label: string }[] = [
  { key: "ott", label: "Driving" },
  { key: "app", label: "Approach" },
  { key: "arg", label: "Around green" },
  { key: "putt", label: "Putting" },
];

export default function PlayerCard(props: PlayerCardProps) {
  const { field, player: p } = props;

  if (!p) {
    return (
      <div
        style={{
          width: 448,
          flex: "none",
          background: c.panel,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          padding: 30,
          textAlign: "center",
          color: c.dim,
          fontSize: 12.5,
        }}
      >
        Select a player in the grid to load their detail card.
      </div>
    );
  }

  const flags = playerFlags(p, field);
  const valPct = field.pct.VAL[p.id] ?? 0;
  const hasPhases = PHASE_ROWS.some((r) => field.pct[r.key][p.id] !== undefined);

  return (
    <div style={{ width: 448, flex: "none", background: c.panel, overflowY: "auto" }}>
      {/* (a) header + stat cards + actions */}
      <div style={{ padding: "14px 16px", borderBottom: `1px solid ${c.line}` }}>
        <div
          style={{
            display: "flex",
            alignItems: "baseline",
            justifyContent: "space-between",
            marginBottom: 12,
            gap: 10,
          }}
        >
          <div style={{ fontSize: 19, fontWeight: 600, letterSpacing: "-0.01em" }}>
            {p.PLAYER}
          </div>
          <div
            style={{
              fontFamily: font.mono,
              fontSize: 11,
              color: c.dim,
              whiteSpace: "nowrap",
            }}
          >
            RANK {p.rank} / {field.players.length}
          </div>
        </div>

        <StatCardRow>
          <StatCard label="P(TOP-20)" value={`${(p.P_TOP20 * 100).toFixed(1)}%`} color={c.green} />
          <StatCard label="SALARY" value={fmtSalary(p.SALARY)} />
          <StatCard
            label="VAL /$1K"
            value={p.VAL.toFixed(2)}
            color={valPct >= 0.85 ? c.green : valPct <= 0.15 ? c.red : c.text}
          />
          {/* Blue so it reads as a market signal, distinct from the green
              model numbers. */}
          <StatCard label="VEGAS ODDS" value={fmtOdds(p.VEGAS_ODDS)} color={c.blue} />
        </StatCardRow>

        <div style={{ display: "flex", gap: 6, marginTop: 10 }}>
          <CardBtn
            active={props.inLineup}
            onClick={() => props.onTogglePick(p.id)}
            label={props.inLineup ? "In lineup" : "Add to lineup"}
          />
          <CardBtn active={props.locked} onClick={() => props.onToggleLock(p.id)} label="Lock" />
          <CardBtn
            active={props.excluded}
            danger
            onClick={() => props.onToggleExclude(p.id)}
            label="Exclude"
          />
        </div>
      </div>

      {/* (b) flags */}
      <Section title="Flags" sub={`vs ${field.players.length} field players`}>
        <div style={{ display: "flex", flexDirection: "column", gap: 7 }}>
          {flags.map((f, i) => (
            <FlagRow key={i} severity={f.severity} text={f.text} />
          ))}
        </div>
      </Section>

      {/* (c) form profile */}
      {/* Windows are labelled explicitly. CUTS here is the last-20-starts rate
          from the results table; the flags and the percentile row use
          CUT_PERCENTAGE, which is a 9-month window. Two different numbers
          describing cuts must never appear unlabelled next to each other — an
          early prototype build showed "100% cuts made" above "CUTS /20 95%"
          and read as the app contradicting itself. */}
      <Section title="Form profile" sub="last 20 starts">
        <div style={{ display: "grid", gridTemplateColumns: "repeat(5, 1fr)", gap: 8 }}>
          <Cell label="SG FORM" value={fmtSigned(p.SG_FORM, 2)} color={p.SG_FORM >= 0 ? c.green : c.red} />
          <Cell label="RNDS 12M" value={p.form?.rounds_12m?.toFixed(0) ?? EM_DASH} />
          <Cell
            label="CUTS /20"
            value={p.form?.cuts_20 != null ? `${p.form.cuts_20.toFixed(0)}%` : EM_DASH}
          />
          <Cell label="STREAK" value={streakText(p) ?? EM_DASH} />
          <Cell
            label="TOP-20 /20"
            value={p.form?.top20_20 != null ? `${p.form.top20_20.toFixed(0)}%` : EM_DASH}
          />
        </div>
      </Section>

      {/* (d) SG by phase — season */}
      <Section
        title={`SG by phase — ${field.meta.season}`}
        sub={hasPhases ? undefined : "no season stats"}
      >
        {hasPhases ? (
          <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
            {PHASE_ROWS.map((row) => (
              <PhaseBar key={row.key} row={row} p={p} field={field} />
            ))}
          </div>
        ) : (
          <div style={{ fontSize: 12, color: c.dim }}>
            No {field.meta.season} PGA Tour stats for this player.
          </div>
        )}
      </Section>

      {/* (e) SG per round — the handoff's retitle to "per event" is wrong;
          build_rounds() derives real per-round SG from ROUNDS:1..4. */}
      <Section title="SG per round — 24 mo" sub={<span>rolling form ——</span>}>
        <SgScatter rounds={p.form?.rounds ?? []} />
      </Section>

      {/* (f) percentile vs field */}
      <Section title="Percentile vs field">
        <div style={{ display: "flex", flexDirection: "column" }}>
          <Pct field={field} p={p} m="P_TOP20" label="P(top-20)" value={`${(p.P_TOP20 * 100).toFixed(1)}%`} />
          <Pct field={field} p={p} m="VAL" label="Value/$1k" value={p.VAL.toFixed(2)} />
          <Pct field={field} p={p} m="SG_FORM" label="SG form" value={fmtSigned(p.SG_FORM, 2)} />
          <Pct field={field} p={p} m="SG_CH_SHRUNK" label="SG course" value={fmtSigned(p.SG_CH_SHRUNK, 2)} />
          <Pct field={field} p={p} m="CUT_PERCENTAGE" label="Cut % 9mo" value={`${p.CUT_PERCENTAGE.toFixed(0)}%`} />
          <Pct field={field} p={p} m="LEVERAGE" label="Leverage" value={fmtSigned(p.LEVERAGE, 1)} />
          <Pct field={field} p={p} m="SALARY" label="Salary" value={fmtSalary(p.SALARY)} />
        </div>
      </Section>

      {/* (g) course history */}
      <Section title="Course history" sub="◆ = this week">
        <CourseHistory p={p} thisCourse={field.meta.course} />
      </Section>

      {/* (h) recent results */}
      <Section title="Recent results" last>
        <RecentResults p={p} />
      </Section>
    </div>
  );
}

function streakText(p: Player): string | null {
  const s = p.form?.streak;
  if (!s) return null;
  return `${s[0]} ${s[1] === "made" ? "made" : "missed"}`;
}

function Pct({
  field,
  p,
  m,
  label,
  value,
}: {
  field: Field;
  p: Player;
  m: Metric;
  label: string;
  value: string;
}) {
  return (
    <PercentileBar label={label} value={value} pct={field.pct[m][p.id]} rank={field.rnk[m][p.id]} />
  );
}

/** Zero line sits at 38% so negative bars have room without the label column
 *  shifting. Values clamp at +/-1.4 strokes. */
const ZERO_AT = 38;
const CLAMP = 1.4;

function PhaseBar({
  row,
  p,
  field,
}: {
  row: { key: Metric; label: string };
  p: Player;
  field: Field;
}) {
  const v = p.form?.phases?.[row.key as "ott" | "app" | "arg" | "putt"] ?? null;
  const rank = field.rnk[row.key][p.id];
  const mag = v === null ? 0 : Math.min(Math.abs(v), CLAMP) / CLAMP;
  const positive = (v ?? 0) >= 0;
  const color = positive ? c.green : c.red;

  return (
    <div style={{ display: "grid", gridTemplateColumns: "86px 1fr 96px", alignItems: "center", gap: 8 }}>
      {/* 86px specifically so "Around green" stays on one line. */}
      <div style={{ fontFamily: font.mono, fontSize: 11, color: c.muted, whiteSpace: "nowrap" }}>
        {row.label}
      </div>
      <div style={{ position: "relative", height: 16, background: c.surface, borderRadius: 3 }}>
        <div
          style={{
            position: "absolute",
            left: `${ZERO_AT}%`,
            top: 0,
            bottom: 0,
            width: 1,
            background: c.axis,
          }}
        />
        {v !== null && (
          <div
            style={{
              position: "absolute",
              top: 3,
              bottom: 3,
              borderRadius: 2,
              background: color,
              ...(positive
                ? { left: `${ZERO_AT}%`, width: `${mag * (100 - ZERO_AT)}%` }
                : { right: `${100 - ZERO_AT}%`, width: `${mag * ZERO_AT}%` }),
            }}
          />
        )}
      </div>
      <div style={{ fontFamily: font.mono, fontSize: 11, textAlign: "right" }}>
        {v === null ? (
          <span style={{ color: c.dimmer }}>{EM_DASH}</span>
        ) : (
          <>
            <span style={{ color }}>{fmtSigned(v, 2)}</span>{" "}
            <span style={{ color: c.dim }}>#{rank}</span>
          </>
        )}
      </div>
    </div>
  );
}

function Cell({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div>
      <div style={{ fontFamily: font.mono, fontSize: 9, color: c.dim, letterSpacing: "0.06em" }}>
        {label}
      </div>
      <div
        style={{
          fontFamily: font.mono,
          fontSize: 16,
          fontWeight: 600,
          whiteSpace: "nowrap",
          color: color ?? (value === EM_DASH ? c.dimmer : c.text),
        }}
      >
        {value}
      </div>
    </div>
  );
}

function CardBtn({
  label,
  active,
  danger,
  onClick,
}: {
  label: string;
  active: boolean;
  danger?: boolean;
  onClick: () => void;
}) {
  return (
    <button
      onClick={onClick}
      style={{
        flex: 1,
        padding: 8,
        borderRadius: 4,
        border: `1px solid ${active ? (danger ? c.red : c.green) : c.lineStrong}`,
        background: active ? (danger ? c.red : c.green) : "transparent",
        color: active ? "#0b0d10" : c.text2,
        fontSize: 12,
        fontWeight: 500,
        textAlign: "center",
        fontFamily: font.sans,
        cursor: "pointer",
      }}
    >
      {label}
    </button>
  );
}
