import { c, font, tier, dirColor, rankColor } from "../tokens";
import type { Field, Player, Metric } from "../enrich";
import { playerFlags } from "../flags";
import { Section, StatCard, StatCardRow, FlagRow, PercentileBar } from "../components/primitives";
import { fmtSalary, fmtSigned, fmtOdds, EM_DASH } from "../format";
import { SgScatter, SgScatterLegend, CourseHere, RecentResults } from "./CardHistory";

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
  /** All three are read-only here — set them with L / X on the grid row. */
  inLineup: boolean;
  locked: boolean;
  excluded: boolean;
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
          background: c.bg,
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
    // The column does not scroll; its lower half does. The header used to be
    // `position:sticky` inside a scrolling column, which pins it correctly but
    // leaves the scrollbar running the full height of the column — past a header
    // that never moves. A flex column with a `flex:none` header and a `flex:1`
    // scrolling body pins the same content and confines the track to the part
    // that actually scrolls. `minHeight:0` is what lets the body shrink below
    // its content height instead of overflowing the column.
    <div
      style={{
        width: 448,
        flex: "none",
        background: c.bg,
        display: "flex",
        flexDirection: "column",
        minHeight: 0,
      }}
    >
      {/* (a) header + stat cards + actions — pinned. Identity (who this is) and
          the two constraints you set on him stay reachable no matter how far
          down the history you have scrolled; full-bleed and un-carded so it
          reads as the frame around the cards rather than the first of them.

          The 2px blue inset edge is the grid row's lineup edge, repeated here:
          same signal, same shape, same colour, so the card and the row he came
          from agree at a glance about whether he is in the build. */}
      <div
        style={{
          flex: "none",
          background: c.panel,
          padding: "14px 16px",
          borderBottom: `1px solid ${c.line}`,
          boxShadow: props.inLineup ? `inset 2px 0 0 ${c.blue}` : undefined,
        }}
      >
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
          {/* State is stated, not just tinted: the blue edge says "this player
              is special somehow", the chip says which way. These are the three
              things the buttons used to report by lighting up, in the same
              three colours the grid uses — read-only now, since L and X on the
              row are the only place either constraint is set. They sit with
              RANK because all four are facts about where he stands. */}
          <div style={{ display: "flex", alignItems: "baseline", gap: 6, whiteSpace: "nowrap" }}>
            {props.inLineup && <Chip label="IN LINEUP" color={c.blue} />}
            {props.locked && <Chip label="LOCKED" color={c.green} />}
            {props.excluded && <Chip label="EXCLUDED" color={c.red} />}
            <div style={{ fontFamily: font.mono, fontSize: 11, color: c.dim, marginLeft: 2 }}>
              RANK {p.rank} / {field.players.length}
            </div>
          </div>
        </div>

        {/* Each tile takes the SAME colour its column takes in the grid, from
            the same helper — so a player who is green in the VAL column is green
            in the VAL tile, and clicking a row never changes what a colour
            means. P(TOP-20) used to be unconditionally green here and
            conditionally green there, which is the mismatch that made the pair
            feel arbitrary. Odds and salary have no verdict, so they tier. */}
        <StatCardRow>
          <StatCard
            label="P(TOP-20)"
            value={`${(p.P_TOP20 * 100).toFixed(1)}%`}
            color={rankColor(field.pct.P_TOP20[p.id])}
          />
          <StatCard label="SALARY" value={fmtSalary(p.SALARY)} />
          <StatCard label="VAL /$1K" value={p.VAL.toFixed(2)} color={rankColor(valPct)} />
          <StatCard
            label="VEGAS ODDS"
            value={fmtOdds(p.VEGAS_ODDS)}
            color={tier(field.pct.VEGAS_ODDS[p.id])}
          />
        </StatCardRow>

        {/* The Add / Lock / Exclude row is gone. Lock and Exclude were the
            grid's L and X a second time over, and "Add to lineup" was worse
            than redundant: on an empty roster it filled a slot the next
            Optimize threw away (only LOCKS survive a solve), and on a full one
            it did nothing at all. Every action now lives in exactly one place —
            the row — and the card is what it says it is: the read-out. The
            ~44px this frees goes to the history below. */}
      </div>

      {/* Each remaining section is its own card. The 10px gap of app background
          between them is the divider — a gap separates more cleanly than a rule
          because it costs no ink and cannot be mistaken for a table border. */}
      <div
        style={{
          flex: 1,
          minHeight: 0,
          overflowY: "auto",
          padding: 10,
          display: "flex",
          flexDirection: "column",
          gap: 10,
        }}
      >
        {/* (b) flags */}
        <Section card title="Flags" sub={`vs ${field.players.length} field players`}>
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
        <Section card title="Form profile" sub="last 20 starts">
          <div style={{ display: "grid", gridTemplateColumns: "repeat(5, 1fr)", gap: 8 }}>
            {/* SG form is the only one of the five with a sign, so it is the
                only one with a hue. Rounds, cuts, streak and top-20 rate are
                counts — high is not "good", it is just high. */}
            <Cell label="SG FORM" value={fmtSigned(p.SG_FORM, 2)} color={dirColor(p.SG_FORM)} />
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
          card
          title={`SG by phase — ${field.meta.season}`}
          sub={
            hasPhases
              ? `scale ${fmtSigned(-field.phaseScale.negMax, 1)} … ${fmtSigned(field.phaseScale.posMax, 1)} (field)`
              : "no season stats"
          }
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
        <Section card title="SG per round — 24 mo" sub={<SgScatterLegend />}>
          <SgScatter rounds={p.form?.rounds ?? []} />
        </Section>

        {/* (f) percentile vs field */}
        {/* Deliberately five rows, not seven. P(top-20) is the number the whole
            card is sorted by and already sits in the header; Value, Leverage and
            Salary are all inputs the optimizer acts on directly, so ranking them
            by eye adds nothing you would act on. What is left is the stuff that
            is genuinely about the golfer rather than about the slate — plus two
            measures nothing else here states: how streaky he is, and which way
            he is trending. */}
        <Section card title="Percentile vs field">
          <div style={{ display: "flex", flexDirection: "column" }}>
            <Pct field={field} p={p} m="SG_FORM" label="SG form" value={fmtSigned(p.SG_FORM, 2)} />
            <Pct field={field} p={p} m="SG_CH_SHRUNK" label="SG course" value={fmtSigned(p.SG_CH_SHRUNK, 2)} />
            <Pct field={field} p={p} m="CUT_PERCENTAGE" label="Cut % 9mo" value={`${p.CUT_PERCENTAGE.toFixed(0)}%`} />
            <Pct
              field={field}
              p={p}
              m="momentum"
              label="Momentum 90d"
              value={p.form?.momentum != null ? fmtSigned(p.form.momentum, 2) : EM_DASH}
            />
            <Pct
              field={field}
              p={p}
              m="volatility"
              label="Volatility"
              value={p.form?.volatility != null ? p.form.volatility.toFixed(2) : EM_DASH}
              neutral
            />
          </div>
        </Section>

        {/* (g) record at this week's course */}
        <Section card title="At this course" sub={field.meta.course}>
          <CourseHere p={p} thisCourse={field.meta.course} />
        </Section>

        {/* (h) recent results */}
        <Section card title="Recent results">
          <RecentResults p={p} />
        </Section>
      </div>
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
  neutral,
}: {
  field: Field;
  p: Player;
  m: Metric;
  label: string;
  value: string;
  neutral?: boolean;
}) {
  return (
    <PercentileBar
      label={label}
      value={value}
      pct={field.pct[m][p.id]}
      rank={field.rnk[m][p.id]}
      neutral={neutral}
    />
  );
}

/**
 * Bars are scaled to the FIELD's extremes (field.phaseScale), not to a fixed
 * clamp. The best player in the field reaches the right edge, the worst reaches
 * the left, and everyone else is the proportional fraction of that span — so no
 * space is wasted on a range nobody occupies, and the scale holds still while
 * you toggle between players.
 */
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
  const { posMax, negMax, zeroAt } = field.phaseScale;
  const positive = (v ?? 0) >= 0;
  const frac = v === null ? 0 : positive ? v / posMax : -v / negMax;
  const color = dirColor(v);

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
            left: `${zeroAt}%`,
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
                ? { left: `${zeroAt}%`, width: `${frac * (100 - zeroAt)}%` }
                : { right: `${100 - zeroAt}%`, width: `${frac * zeroAt}%` }),
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

/** Outlined, not filled: a chip you cannot press must not look like the solid
 *  buttons on the rail. The hue carries the meaning; the outline says read-only. */
function Chip({ label, color }: { label: string; color: string }) {
  return (
    <span
      style={{
        fontFamily: font.mono,
        fontSize: 9,
        letterSpacing: "0.08em",
        color,
        border: `1px solid ${color}`,
        borderRadius: 3,
        padding: "2px 5px",
      }}
    >
      {label}
    </span>
  );
}
