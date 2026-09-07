import { c, font, radius, stretch, type as t, weight } from "../tokens";
import type { SlateMeta } from "../types";
import { fmtDate } from "../format";

/**
 * THE TOP BAR — two rows, and the split is the point.
 *
 * ROW 1 IS WHO YOU ARE AND WHERE YOU CAN GO: the wordmark and the nav. It is
 * the same on every tab and it never changes within a week.
 * ROW 2 IS WHAT YOU ARE LOOKING AT: the tournament, its course, the date, and
 * the size of the field.
 *
 * They used to share one row — the wordmark stacked over the tournament in a
 * 250px block on the left, with the tabs squeezed into whatever was left. That
 * arrangement had a documented history of the tabs starving (the handoff's
 * "never add a fourth flex:none child"), and it read wrong besides: a fixed
 * label and a weekly fact stacked in one column, with navigation treated as the
 * thing that could be pushed off screen. Six tabs and a search box do not fit
 * beside a two-line brand block, which is why it kept failing.
 *
 * SEPARATED, THE PRESSURE IS GONE. Row 1 carries the wordmark, the tabs and the
 * search box, and the tabs are the only flexible child — the one constraint from
 * the handoff that survives, because it is the one that was actually load-bearing.
 * Row 2 has a whole line for four short facts and nothing competing for it, so
 * the tournament name can take `type.head` and read as the heading it is.
 *
 * WHY THE SLATE LINE IS DRAWN ON EVERY TAB, not just This Week: every tab in
 * this app is about one tournament. The Course tab is THIS course, SG Form is
 * this field, the Tracker is the weeks up to this one. A line that vanished on
 * five of six tabs would be answering "which week is this" only on the tab where
 * you were least likely to have forgotten.
 */

export type Tab = "slate" | "course" | "sg" | "tracker" | "results" | "query";

/** Tabs needing a fetch(), and therefore a server. Gated on file://. */
const NEEDS_HTTP: Tab[] = ["results", "query"];

const TABS: { id: Tab; label: string }[] = [
  { id: "slate", label: "This Week" },
  { id: "course", label: "Course" },
  { id: "sg", label: "SG Form" },
  { id: "tracker", label: "Prediction Tracker" },
  { id: "results", label: "Results Browser" },
  { id: "query", label: "DB Query" },
];

/** Placeholders name what the search box does IN THIS TAB. One box that means
 *  something different per tab is only usable if it says so. */
const PLACEHOLDER: Record<Tab, string> = {
  slate: "filter the field by player…",
  course: "search is in the table below",
  sg: "search is in the table below",
  tracker: "no search here — the tracker is a time series, not a list",
  results: "search is per-column, in the result table",
  query: "search is per-column, in the result table",
};

/** The box only drives the field grid, so it is inert everywhere else —
 *  GREYED, NEVER REMOVED, or the row's contents would shift on every tab
 *  change. */
const SEARCHABLE: Tab[] = ["slate"];

export default function TopBar({
  meta,
  tab,
  onTab,
  query,
  onQuery,
  resultsEnabled,
}: {
  meta: SlateMeta;
  tab: Tab;
  onTab: (t: Tab) => void;
  query: string;
  onQuery: (q: string) => void;
  resultsEnabled: boolean;
}) {
  const searchable = SEARCHABLE.includes(tab);
  return (
    <div
      // NAMED, NOT COUNTED. Anything addressing this bar by
      // `#root > div > div:nth-child(1)` breaks the day a row is inserted —
      // which is exactly what this change did.
      data-part="topbar"
      style={{
        flex: "none",
        background: c.panel,
        borderBottom: `1px solid ${c.line}`,
      }}
    >
      {/* ---- ROW 1: the wordmark, the nav, the search --------------------- */}
      <div
        data-part="navrow"
        style={{
          display: "flex",
          alignItems: "center",
          gap: 18,
          padding: "0 16px",
          height: 46,
        }}
      >
        <div
          style={{
            fontFamily: font.sans,
            fontSize: t.name,
            fontWeight: weight.bold,
            // The wordmark is the app's most structural label, so it takes the
            // width axis further than a micro-label does — see tokens.stretch.
            fontStretch: "122%",
            letterSpacing: "0.15em",
            color: c.text,
            whiteSpace: "nowrap",
          }}
        >
          PGA SLATE TERMINAL
        </div>

        {/* THE ONLY FLEXIBLE CHILD, and that is the handoff's rule kept. The
            children's intrinsic widths exceed a narrow viewport, and whichever
            child is flex:none pushes the others off-screen; the tab group is
            the one that may shrink and scroll instead. */}
        <div
          style={{
            flex: "1 1 auto",
            minWidth: 0,
            overflowX: "auto",
            display: "flex",
            gap: 2,
          }}
        >
          {TABS.map((tb) => {
            const disabled = NEEDS_HTTP.includes(tb.id) && !resultsEnabled;
            const on = tab === tb.id;
            return (
              <button
                key={tb.id}
                // `.dimhover` owns the resting and hover colour of an INACTIVE
                // tab — an inline colour could never be brightened by a :hover
                // rule. The active tab's colour is set inline below, so it
                // stays brightest regardless of where the mouse is.
                className={disabled ? undefined : "dimhover"}
                onClick={() => !disabled && onTab(tb.id)}
                title={
                  disabled
                    ? "Needs the page served over http — open it from the notebook's serve cell."
                    : undefined
                }
                style={{
                  flex: "none",
                  // NO WASH ON THE ACTIVE TAB. The underline below already says
                  // which one is on, and a fill behind it made the loudest thing
                  // in the bar a navigation control — on a screen whose whole job
                  // is to let a NUMBER stand out.
                  background: "transparent",
                  border: "none",
                  // Position, not a value: an underline rather than a fill.
                  boxShadow: on ? `inset 0 -2px 0 ${c.blue}` : undefined,
                  color: disabled ? c.axis : on ? c.text : undefined,
                  fontFamily: font.sans,
                  fontSize: t.name,
                  fontWeight: on ? weight.semi : weight.medium,
                  whiteSpace: "nowrap",
                  padding: "13px 13px",
                  cursor: disabled ? "not-allowed" : "pointer",
                }}
              >
                {tb.label}
              </button>
            );
          })}
        </div>

        <input
          data-part="filter"
          value={query}
          onChange={(e) => onQuery(e.target.value)}
          placeholder={PLACEHOLDER[tab]}
          disabled={!searchable}
          style={{
            flex: "none",
            width: 240,
            background: c.surfaceAlt,
            // BLUE WHILE IT HOLDS SOMETHING, because a filter left set is the
            // single easiest way to look at the wrong field and not know it.
            border: `1px solid ${query ? c.blue : c.lineStrong}`,
            borderRadius: radius.md,
            padding: "7px 11px",
            fontSize: t.data,
            fontFamily: font.sans,
            color: c.text,
            outline: "none",
            opacity: searchable ? 1 : 0.4,
          }}
        />
      </div>

      {/* ---- ROW 2: which tournament this is ----------------------------- */}
      <div
        data-part="slaterow"
        style={{
          display: "flex",
          alignItems: "baseline",
          gap: 12,
          padding: "0 16px 9px",
          flexWrap: "wrap",
        }}
      >
        <div
          style={{
            fontFamily: font.sans,
            fontSize: t.head,
            fontWeight: weight.bold,
            letterSpacing: "-0.01em",
            color: c.text,
          }}
        >
          {meta.tournament}
        </div>
        {/* THE COURSE IS A PLACE, NOT A NUMBER, so it is sans at `muted` — one
            step under the name it belongs to rather than a second heading. */}
        <div style={{ fontFamily: font.sans, fontSize: t.body, color: c.muted }}>
          {meta.course}
        </div>
        <div style={{ fontFamily: font.data, fontSize: t.small, color: c.dim }}>
          {fmtDate(meta.ending_date)}
        </div>
        <div style={{ flex: 1 }} />
        {/* THE FIELD SIZE, at the far end. It is the denominator every ramp in
            the app is measured against — "top fifth of the field" means nothing
            until you know how big the field is. */}
        <div
          data-part="fieldsize"
          title="Players in this week's field — the population every column's colour ramp is ranked against."
          style={{ fontFamily: font.data, fontSize: t.chip, color: c.dim, whiteSpace: "nowrap" }}
        >
          <span style={{ letterSpacing: "0.06em", fontStretch: stretch.label }}>FIELD </span>
          {meta.field_size}
        </div>
      </div>
    </div>
  );
}
