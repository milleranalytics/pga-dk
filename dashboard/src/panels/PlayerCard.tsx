import { useEffect, useState } from "react";
import { c, font, dirColor, rankColor } from "../tokens";
import type { Field, Player, Metric } from "../enrich";
import { playerFlags, FLAG_GUIDE } from "../flags";
import {
  Section,
  StatCard,
  StatCardRow,
  FlagRow,
  PercentileBar,
  SeverityDot,
  microLabel,
} from "../components/primitives";
import { fmtSalary, fmtSigned, fmtOdds, EM_DASH } from "../format";
import { SgScatter, SgScatterLegend, CourseHere, RecentResults } from "./CardHistory";

/**
 * The player card — (a) header/stats, (b) flags, (c) form profile,
 * (d) strokes gained, (e) SG per round, (f) percentile vs field,
 * (g) record at this course, (h) recent results.
 *
 * Nothing here is ever stubbed with placeholder data; a section with no data
 * says so in words. The handoff's consistency rule exists because an early
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

/**
 * Tee-to-green leads, then the parts.
 *
 * T2G is the composite of the first three (driving + approach + around-green,
 * everything but putting) and it is the single most-checked read on a golfer, so
 * it belongs at the top of the section rather than only inside the flag engine.
 * It is marked `total` so it can be drawn as the summary of the rows beneath it
 * — an unmarked sum sitting in a list of its own parts invites reading five
 * independent measurements and adding them up.
 */
const SG_ROWS: { key: Metric; label: string; total?: boolean }[] = [
  { key: "ttg", label: "Tee to green", total: true },
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
  const hasPhases = SG_ROWS.some((r) => field.pct[r.key][p.id] !== undefined);

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
            the same helper and the same percentile — so a player who is green in
            the VAL column is green in the VAL tile, and clicking a row never
            changes what a colour means. Odds went through a separate grey-only
            ramp until the grid put every column on rankColor; it now matches its
            column too. Salary is exception 2 and stays uncoloured in both. */}
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
            color={rankColor(field.pct.VEGAS_ODDS[p.id])}
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
        {/* The one panel on the card that reaches a VERDICT rather than
            reporting a number, which is exactly why it needs to be able to
            explain itself: a flag you cannot audit is a flag you either trust
            blindly or ignore. The reference is one hover away and never further
            than that — it does not open a tab, and it does not take space away
            from the flags themselves when you are not asking. */}
        <Section
          card
          title="Flags"
          sub={
            <span style={{ display: "inline-flex", alignItems: "center", gap: 7 }}>
              vs {field.players.length} field players
              <FlagGuide />
            </span>
          }
        >
          <div style={{ display: "flex", flexDirection: "column", gap: 7 }}>
            {flags.map((f, i) => (
              <FlagRow key={i} severity={f.severity} text={f.text} />
            ))}
          </div>
        </Section>

        {/* (c) form profile — FOUR cells, not five.
            Windows are labelled explicitly. CUTS here is the last-20-starts rate
            from the results table; the flags and the percentile row use
            CUT_PERCENTAGE, which is a 9-month window. Two different numbers
            describing cuts must never appear unlabelled next to each other — an
            early prototype build showed "100% cuts made" above "CUTS /20 95%"
            and read as the app contradicting itself.

            RNDS 12M was the fifth cell and is gone. It was the only one that was
            neither a result nor a rate: it said how much golf got played, which
            the SG-per-round scatter directly below shows as its own point count,
            and which the flag engine already raises as "Thin sample" on the one
            player in twelve where it changes how the rest should be read. The
            number is still in the payload and still drives that flag — it just
            no longer spends a fifth of this row restating what two other things
            on the card say better.

            Nothing was promoted into the free space, and that is a decision
            rather than an omission. Every candidate already appears a few
            hundred pixels down: momentum and volatility both sit in Percentile
            vs field, where they carry a field rank this row has no column for. A
            duplicate is a worse tenant than an empty seat. Four cells at 1/4
            width also breathe better than five at 1/5 — STREAK's "6 missed"
            stops being the string that sets the column width. */}
        <Section card title="Form profile" sub="last 20 starts">
          <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 8 }}>
            {/* SG form is the only one of the four the field is ranked on, so it
                is the only one on the ramp. It is also the SAME number as the
                grid's SG:F column and the Percentile vs field row below, which
                is why it cannot keep colouring by sign while those two colour by
                rank — one quantity, three places, one colour. Cuts, streak and
                top-20 rate carry no field percentile and stay uncoloured. */}
            <Cell
              label="SG FORM"
              value={fmtSigned(p.SG_FORM, 2)}
              color={rankColor(field.pct.SG_FORM[p.id])}
            />
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

        {/* (d) strokes gained — season */}
        <Section
          card
          title={`Strokes gained — ${field.meta.season}`}
          sub={
            hasPhases
              ? `scale ${fmtSigned(-field.phaseScale.negMax, 1)} … ${fmtSigned(field.phaseScale.posMax, 1)} (field)`
              : "no season stats"
          }
        >
          {hasPhases ? (
            <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
              {SG_ROWS.map((row) => (
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
  row: { key: Metric; label: string; total?: boolean };
  p: Player;
  field: Field;
}) {
  const v = p.form?.phases?.[row.key as "ttg" | "ott" | "app" | "arg" | "putt"] ?? null;
  const rank = field.rnk[row.key][p.id];
  const { posMax, negMax, zeroAt } = field.phaseScale;
  const positive = (v ?? 0) >= 0;
  const frac = v === null ? 0 : positive ? v / posMax : -v / negMax;
  const color = dirColor(v);

  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "86px 1fr 96px",
        alignItems: "center",
        gap: 8,
        // The total is set off from its own components by a rule and 5px, the
        // way a subtotal is: enough to say "the four below make this up", not
        // enough to look like a second section.
        ...(row.total
          ? { paddingBottom: 5, marginBottom: 1, borderBottom: `1px solid ${c.lineSoft}` }
          : null),
      }}
    >
      {/* 86px specifically so "Around green" stays on one line. */}
      {/* Every label is the same weight, including the total's. The rule under
          the T2G row is doing the whole job of saying "the four below make this
          up", and brightening the label as well was the same message twice —
          which read as T2G being a more IMPORTANT metric rather than a
          different KIND of one. One signal, one meaning. */}
      <div
        style={{
          fontFamily: font.mono,
          fontSize: 11,
          color: c.muted,
          whiteSpace: "nowrap",
        }}
      >
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

/**
 * The Flags reference — every rule and every threshold, on hover.
 *
 * Content comes from FLAG_GUIDE in flags.ts, whose numbers are interpolated
 * from the THRESHOLDS object the engine itself reads. Retune a threshold and
 * this panel moves with it; there is no second copy of the rules to forget.
 *
 * CLICK to open, and it stays open until you dismiss it — it does not close on
 * mouse-out. It was a hover flyout first, and that was wrong for the one thing
 * this panel is actually for: the content is longer than the panel, so it has
 * to be scrolled, and a panel that vanishes when the pointer leaves it is a
 * panel you cannot comfortably reach the scrollbar of. Reference material you
 * read is not a tooltip you glance at.
 *
 * Dismissed by the × , by the ? again, or by Escape. Deliberately NOT by
 * clicking elsewhere: this is reference text with nothing player-specific in
 * it, so leaving it up while you click through players in the grid is a
 * legitimate way to use it, and an outside-click handler would take it away
 * mid-comparison every time.
 */
function FlagGuide() {
  const [open, setOpen] = useState(false);

  // Escape closes it. The listener only exists while it is open, so there is no
  // permanent key handler competing with anything else on the page.
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setOpen(false);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open]);

  return (
    <span style={{ position: "relative", display: "inline-flex", alignItems: "center" }}>
      <button
        onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
        aria-label="What the flags mean"
        title={open ? "Hide the flag reference" : "What fires a flag?"}
        style={{
          fontFamily: font.mono,
          fontSize: 9,
          fontWeight: 600,
          lineHeight: 1,
          width: 14,
          height: 14,
          padding: 0,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          borderRadius: "50%",
          // Reads as pressed while its panel is up, which is the only cue that
          // the panel belongs to this control.
          border: `1px solid ${open ? c.muted : c.lineStrong}`,
          background: "transparent",
          color: open ? c.text2 : c.dim,
          cursor: "pointer",
          userSelect: "none",
        }}
      >
        ?
      </button>

      {open && (
        <div
          style={{
            position: "absolute",
            top: "100%",
            right: 0,
            marginTop: 6,
            // Fits inside the card's 398px of inner width with room to spare —
            // the column clips horizontally, so a wider panel would lose its
            // left edge rather than push the layout.
            width: 384,
            maxHeight: "58vh",
            overflowY: "auto",
            // One step lighter than the cards it covers, with the strong border:
            // that pairing is how everything else here says "on top of", since
            // the palette has no shadows to spend.
            background: c.surface,
            border: `1px solid ${c.lineStrong}`,
            borderRadius: 6,
            // No TOP padding: the sticky header supplies its own, so that the
            // header's margin box starts exactly at the scrollport's top edge.
            // See the header for why that matters.
            padding: "0 14px 14px",
            zIndex: 10,
            textAlign: "left",
            cursor: "default",
          }}
        >
          {/* Sticky so the title and the way out stay reachable at the bottom
              of a panel this long — the close button being scrolled off is the
              same trap as the hover version, one step further in.

              The header owns the panel's top padding rather than the panel
              owning it, and its top margin is ZERO. That is load-bearing:
              `top: 0` constrains the element's MARGIN box to the scrollport's
              top edge, so a negative top margin makes sticky push the header
              DOWN by that amount — which is what happened when the panel kept
              its 12px padding and the header wore `margin-top: -12px` to cover
              it. The header landed 12px low and painted its background over the
              first line of text. With no top margin the margin box already
              starts at the scrollport edge, so the constraint is satisfied at
              rest and the header does not move until you actually scroll.

              The horizontal margins stay negative — those are full-bleed only
              and no constraint applies to them. `padding-bottom` rather than
              `margin-bottom` for the gap below, so the opaque background runs
              all the way to the text and nothing scrolls through a transparent
              sliver under it. */}
          <div
            style={{
              position: "sticky",
              top: 0,
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              gap: 8,
              background: c.surface,
              margin: "0 -14px",
              padding: "12px 14px 10px",
            }}
          >
            <div style={microLabel(0.14)}>What fires a flag</div>
            <button
              onClick={() => setOpen(false)}
              aria-label="Close"
              title="Close (Esc)"
              style={{
                fontFamily: font.mono,
                fontSize: 12,
                lineHeight: 1,
                width: 18,
                height: 18,
                padding: 0,
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                border: `1px solid ${c.lineStrong}`,
                borderRadius: 3,
                background: "transparent",
                color: c.dim,
                cursor: "pointer",
                flex: "none",
              }}
            >
              ×
            </button>
          </div>
          <p
            style={{
              margin: "0 0 12px",
              fontFamily: font.sans,
              fontSize: 11.5,
              lineHeight: 1.45,
              color: c.dim,
            }}
          >
            Percentiles are against THIS field, so they re-scale every week; the
            rest are absolute. Only outliers are listed — a player with nothing to
            say about him shows one grey line, and that is a finding too. Red
            sorts first, then green, then cautions.
          </p>

          {FLAG_GUIDE.map((g) => (
            <div key={g.title} style={{ marginBottom: 12 }}>
              <div style={{ ...microLabel(0.1), color: c.text2, marginBottom: 4 }}>{g.title}</div>
              {g.note && (
                <p
                  style={{
                    margin: "0 0 6px",
                    fontFamily: font.sans,
                    fontSize: 11,
                    lineHeight: 1.4,
                    color: c.dimmer,
                  }}
                >
                  {g.note}
                </p>
              )}
              <div style={{ display: "flex", flexDirection: "column", gap: 5 }}>
                {g.rules.map((r) => (
                  <div key={r.name} style={{ display: "flex", gap: 9, alignItems: "flex-start" }}>
                    <SeverityDot severity={r.severity} />
                    <div style={{ fontFamily: font.sans, fontSize: 11.5, lineHeight: 1.35 }}>
                      <span style={{ color: c.text2, fontWeight: 600 }}>{r.name}</span>{" "}
                      <span style={{ color: c.dim }}>— {r.when}</span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ))}

          {/* Where to go to change any of it. The point of documenting the
              thresholds is being able to argue with them. */}
          <div
            style={{
              borderTop: `1px solid ${c.lineSoft}`,
              paddingTop: 8,
              fontFamily: font.mono,
              fontSize: 10,
              color: c.dimmer,
              lineHeight: 1.4,
            }}
          >
            Every number above is read from THRESHOLDS in src/flags.ts.
          </div>
        </div>
      )}
    </span>
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
