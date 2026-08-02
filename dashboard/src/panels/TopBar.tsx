import { c, font } from "../tokens";
import type { SlateMeta } from "../types";
import { fmtDate } from "../format";

/**
 * Top bar.
 *
 * LAYOUT WARNING from the handoff, preserved deliberately: this bar went
 * through several failed iterations. The children's intrinsic widths exceed
 * narrow viewports, and whichever child is flex:none pushes the others
 * off-screen. The working arrangement is exactly this — brand and filter are
 * flex:none, ONLY the tab group flexes and scrolls. Never add a fourth
 * flex:none child; that starved the tab group until two of three tabs were
 * unreachable.
 *
 * The prototype's A/B layout toggle is dropped, per the handoff ("drop it in
 * production unless the compare board gets built").
 */

export type Tab = "slate" | "tracker" | "results";

const TABS: { id: Tab; label: string }[] = [
  { id: "slate", label: "This Week" },
  { id: "tracker", label: "Prediction Tracker" },
  { id: "results", label: "Results Browser" },
];

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
  return (
    <div
      style={{
        height: 62,
        flex: "none",
        display: "flex",
        alignItems: "stretch",
        background: c.panel,
        borderBottom: `1px solid ${c.line}`,
      }}
    >
      <div style={{ flex: "none", minWidth: 250, padding: "9px 18px", whiteSpace: "nowrap" }}>
        <div
          style={{
            fontFamily: font.mono,
            fontSize: 10,
            fontWeight: 600,
            letterSpacing: "0.18em",
            color: c.green,
          }}
        >
          PGA SLATE TERMINAL
        </div>
        <div style={{ display: "flex", alignItems: "baseline", gap: 10, marginTop: 4 }}>
          <div style={{ fontSize: 17, fontWeight: 600, letterSpacing: "-0.01em" }}>
            {meta.tournament}
          </div>
          <div style={{ fontFamily: font.mono, fontSize: 11, color: c.muted }}>
            {meta.course}
          </div>
          <div style={{ fontFamily: font.mono, fontSize: 11, color: c.dim }}>
            {fmtDate(meta.ending_date)}
          </div>
        </div>
      </div>

      <div
        style={{
          flex: "1 1 auto",
          minWidth: 0,
          overflowX: "auto",
          padding: "0 8px",
          borderLeft: `1px solid ${c.line}`,
          display: "flex",
          alignItems: "center",
          gap: 4,
        }}
      >
        {TABS.map((t) => {
          const disabled = t.id === "results" && !resultsEnabled;
          return (
            <button
              key={t.id}
              onClick={() => !disabled && onTab(t.id)}
              title={
                disabled
                  ? "Needs the page served over http — open it from the notebook's serve cell (Phase 3)."
                  : undefined
              }
              style={{
                flex: "none",
                padding: "7px 13px",
                borderRadius: 4,
                border: "none",
                fontSize: 12,
                fontWeight: 500,
                whiteSpace: "nowrap",
                fontFamily: font.sans,
                cursor: disabled ? "not-allowed" : "pointer",
                background: tab === t.id ? c.selectBg : "transparent",
                color: disabled ? c.axis : tab === t.id ? c.text : c.muted,
              }}
            >
              {t.label}
            </button>
          );
        })}
      </div>

      <div
        style={{
          flex: "none",
          padding: "0 14px",
          borderLeft: `1px solid ${c.line}`,
          display: "flex",
          alignItems: "center",
        }}
      >
        <input
          value={query}
          onChange={(e) => onQuery(e.target.value)}
          placeholder="Filter player…"
          style={{
            width: 170,
            background: c.surfaceAlt,
            border: `1px solid ${c.lineStrong}`,
            borderRadius: 4,
            padding: "7px 10px",
            fontSize: 12,
            color: c.text,
            outline: "none",
            fontFamily: font.sans,
          }}
        />
      </div>
    </div>
  );
}
