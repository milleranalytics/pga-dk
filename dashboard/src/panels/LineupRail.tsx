import { useEffect, useRef, useState } from "react";
import { c, font } from "../tokens";
import type { Field } from "../enrich";
import type { SavedLineup, SyncStatus } from "../persist";
import { fmtSalary } from "../format";

/**
 * The lineup rail.
 *
 * Started as a full-width dock across the bottom and moved to a right rail at
 * the owner's request — the vertical treatment reads clearer, particularly for
 * saved lineups as full cards rather than horizontal chips.
 *
 * Two layout constraints from the handoff, both load-bearing:
 *  - The rail scrolls as a whole column (overflowY on the container).
 *  - The saved list needs a floor. The five fixed-height children sum to ~470px,
 *    which at a short viewport left the flexible saved container 10px in which
 *    to render several hundred px of content. Hence flex:1 0 auto + minHeight.
 */

export interface LineupRailProps {
  field: Field;
  picks: string[];
  locks: Record<string, true>;
  saved: SavedLineup[];
  genCount: number;
  maxExposure: number;
  syncStatus: SyncStatus;
  /** Set only when the last Gen press came up short; null when it did not. */
  genNote: string | null;
  onRemove: (id: string) => void;
  onOptimize: () => void;
  onGenerate: () => void;
  onSave: () => void;
  onClear: () => void;
  onLoadSaved: (l: SavedLineup) => void;
  /** By list position — see the SavedLineup comment on why there is no id. */
  onDeleteSaved: (index: number) => void;
  /** Deletes the whole saved set. Guarded by a confirm step — see ClearAll. */
  onClearSaved: () => void;
}

export default function LineupRail(props: LineupRailProps) {
  const { field, picks, locks, saved } = props;
  const roster = field.meta.roster;
  const cap = field.meta.cap;

  const players = picks.map((id) => field.byId.get(id)).filter(Boolean);
  const salary = players.reduce((a, p) => a + (p?.SALARY ?? 0), 0);
  const sumP20 = players.reduce((a, p) => a + (p?.P_TOP20 ?? 0), 0);
  const remaining = cap - salary;
  const emptySlots = roster - players.length;
  const full = players.length === roster;

  // Remaining salary per empty slot, floored to the nearest 100 — the number
  // used to judge whether a build is still viable.
  const avgLeft = emptySlots > 0 ? Math.floor(remaining / emptySlots / 100) * 100 : null;

  const currentKey = [...picks].sort().join("|");

  const optTip =
    "Optimize: rebuilds the lineup from scratch — the best roster under the cap.\n" +
    "• Locked players are kept; everyone else is cleared and re-solved\n" +
    "• Excluded players are never used\n" +
    "No need to clear first: unlocked slots are replaced.";

  const genTip =
    `Gen ${props.genCount}: solves the ${props.genCount} best distinct lineups under the cap.\n` +
    `• Locked players appear in every lineup\n` +
    `• Excluded players in none\n` +
    `• No player exceeds ${props.maxExposure}% exposure across the full saved set\n` +
    `• Each lineup differs from every other and from everything already saved\n` +
    `Results are appended to SAVED and the current build is left alone.\n` +
    `Fewer than ${props.genCount} added means a constraint ran out — it says which.`;

  return (
    <div
      style={{
        flex: "none",
        width: 278,
        display: "flex",
        flexDirection: "column",
        minHeight: 0,
        overflowY: "auto",
        borderLeft: `1px solid ${c.line}`,
        background: c.panel,
      }}
    >
      <div
        style={{
          padding: "12px 14px 9px",
          display: "flex",
          alignItems: "baseline",
          justifyContent: "space-between",
        }}
      >
        <div
          style={{
            fontFamily: font.mono,
            fontSize: 10,
            fontWeight: 600,
            letterSpacing: "0.14em",
            color: c.muted,
          }}
        >
          LINEUP
        </div>
        <div style={{ fontFamily: font.mono, fontSize: 10, color: c.dim }}>
          {roster} × {fmtSalary(cap)}
        </div>
      </div>

      <SyncBadge status={props.syncStatus} />

      <div style={{ padding: "0 10px" }}>
        {Array.from({ length: roster }, (_, i) => {
          const p = players[i];
          const locked = p ? !!locks[p.id] : false;
          return (
            <div
              key={i}
              onClick={p ? () => props.onRemove(p.id) : undefined}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 9,
                height: 34,
                padding: "0 9px",
                marginBottom: 2,
                borderRadius: 3,
                borderLeft: `2px solid ${p ? (locked ? c.green : c.lineStrong) : "#1a1e24"}`,
                background: p ? c.surface : c.slotEmpty,
                cursor: p ? "pointer" : "default",
              }}
            >
              {p ? (
                <>
                  <div
                    style={{
                      fontFamily: font.sans,
                      fontSize: 12,
                      fontWeight: 500,
                      flex: 1,
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                      whiteSpace: "nowrap",
                    }}
                  >
                    {p.PLAYER}
                  </div>
                  <div style={{ fontFamily: font.mono, fontSize: 11, color: c.muted }}>
                    {fmtSalary(p.SALARY)}
                  </div>
                  <div
                    style={{
                      fontFamily: font.mono,
                      fontSize: 11,
                      color: c.text2,
                      width: 34,
                      textAlign: "right",
                    }}
                  >
                    {(p.P_TOP20 * 100).toFixed(1)}
                  </div>
                </>
              ) : (
                <>
                  <div style={{ fontFamily: font.sans, fontSize: 12, flex: 1, color: c.axis }}>
                    Empty
                  </div>
                  <div style={{ fontFamily: font.mono, fontSize: 11, color: c.axis }}>—</div>
                </>
              )}
            </div>
          );
        })}
      </div>

      <div
        style={{
          margin: "10px 10px 0",
          padding: "10px 12px",
          borderRadius: 4,
          background: c.surface,
          display: "grid",
          gridTemplateColumns: "1fr 1fr",
          gap: "5px 10px",
          fontFamily: font.mono,
        }}
      >
        <Label>SALARY</Label>
        <Label>REMAINING</Label>
        <Value>{fmtSalary(salary)}</Value>
        <Value color={remaining < 0 ? c.red : full ? c.green : c.text}>
          {fmtSalary(remaining)}
        </Value>
        <Label>Σ P(TOP-20)</Label>
        <Label>AVG LEFT</Label>
        <Value>{(sumP20 * 100).toFixed(1)}</Value>
        <Value>{avgLeft === null ? "—" : fmtSalary(avgLeft)}</Value>
      </div>

      <div style={{ padding: 10, display: "flex", flexDirection: "column", gap: 5 }}>
        <button onClick={props.onOptimize} title={optTip} style={primaryBtn}>
          Optimize
        </button>
        <div style={{ display: "flex", gap: 5 }}>
          <button onClick={props.onGenerate} title={genTip} style={{ ...secondaryBtn, flex: 1 }}>
            Gen {props.genCount}
          </button>
          <button onClick={props.onSave} style={{ ...secondaryBtn, flex: 1 }}>
            Save
          </button>
          <button onClick={props.onClear} style={{ ...secondaryBtn, width: 30 }}>
            ✕
          </button>
        </div>
        {/* Same rule as the sync badge: silent when there is nothing to act on.
            A Gen press that delivers all five says nothing at all. */}
        {props.genNote && (
          <div
            style={{
              fontFamily: font.mono,
              fontSize: 9.5,
              lineHeight: 1.5,
              color: c.amber,
            }}
          >
            {props.genNote}
          </div>
        )}
      </div>

      <div
        style={{
          padding: "7px 14px",
          borderTop: `1px solid ${c.line}`,
          display: "flex",
          alignItems: "baseline",
          justifyContent: "space-between",
        }}
      >
        <div
          style={{
            fontFamily: font.mono,
            fontSize: 10,
            fontWeight: 600,
            letterSpacing: "0.14em",
            color: c.muted,
          }}
          title={genTip}
        >
          SAVED {saved.length}
        </div>
        {saved.length === 0 ? (
          <div style={{ fontFamily: font.mono, fontSize: 10, color: c.axis }}>
            none yet — Optimize, then Save
          </div>
        ) : (
          <ClearAll count={saved.length} onConfirm={props.onClearSaved} />
        )}
      </div>

      <div style={{ flex: "1 0 auto", minHeight: 126, padding: "0 10px 10px" }}>
        {saved.map((l, i) => {
          const ps = l.ids.map((id) => field.byId.get(id)).filter(Boolean);
          const sal = ps.reduce((a, p) => a + (p?.SALARY ?? 0), 0);
          const p20 = ps.reduce((a, p) => a + (p?.P_TOP20 ?? 0), 0);
          const isCurrent = [...l.ids].sort().join("|") === currentKey;
          return (
            <div
              // The players, not the position: keyed by index, deleting a card
              // would make React reuse the deleted one's DOM for its neighbour.
              key={[...l.ids].sort().join("|")}
              onClick={() => props.onLoadSaved(l)}
              style={{
                padding: "8px 10px",
                marginTop: 6,
                borderRadius: 4,
                border: `1px solid ${isCurrent ? c.blue : c.lineStrong}`,
                background: isCurrent ? c.lineupBg : c.surface,
                cursor: "pointer",
              }}
            >
              <div
                style={{
                  display: "flex",
                  alignItems: "baseline",
                  justifyContent: "space-between",
                  fontFamily: font.mono,
                  fontSize: 11,
                }}
              >
                <span style={{ color: c.muted }}>L{i + 1}</span>
                <span style={{ display: "flex", gap: 9 }}>
                  <span style={{ color: c.text2 }}>{fmtSalary(sal)}</span>
                  <span style={{ color: c.text2 }}>{(p20 * 100).toFixed(1)}</span>
                  <span
                    onClick={(e) => {
                      e.stopPropagation();
                      props.onDeleteSaved(i);
                    }}
                    style={{ color: c.dim, cursor: "pointer" }}
                  >
                    ✕
                  </span>
                </span>
              </div>
              <div style={{ fontSize: 11, color: c.dim, lineHeight: 1.45, marginTop: 3 }}>
                {ps.map((p) => p?.PLAYER).join(" · ")}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

/**
 * Silent while sync is healthy; speaks only when there is something to do
 * about it.
 *
 * Sync cannot fail quietly — a failed write sets "error" — so a permanent
 * "synced" line costs a row of rail height to say nothing actionable, and
 * "saving…"/"saved" alternate on every edit, which reads as flicker rather
 * than as information. The two states below are the ones that change what the
 * user should do: `local` means nothing is being written at all, `error` means
 * writes are being rejected and the work is stranded in this browser.
 *
 * Nothing is lost by dropping the timestamp with them: the file's own saved_at
 * is what arbitrates which copy wins on load, and it is still written.
 */
function SyncBadge({ status }: { status: SyncStatus }) {
  const text: Partial<Record<SyncStatus, string>> = {
    local: "this browser only — not syncing",
    error: "sync failed — browser copy kept",
  };
  const color: Partial<Record<SyncStatus, string>> = {
    local: c.dim,
    error: c.amber,
  };

  const label = text[status];
  if (!label) return null;   // idle / saving / saved — healthy, so say nothing
  return (
    <div
      style={{
        padding: "0 14px 8px",
        fontFamily: font.mono,
        fontSize: 9.5,
        letterSpacing: "0.04em",
        color: color[status],
        display: "flex",
        alignItems: "center",
        gap: 6,
      }}
      title={
        status === "local"
          ? "Opened straight from disk, so only this browser has the lineups. Run the notebook's last cell (serve_dashboard) and open the link it prints to sync them."
          : "Could not write current.json in OneDrive — check that the server window is still open. The lineups are safe in this browser meanwhile, and will sync on the next successful save."
      }
    >
      <span
        style={{
          width: 5,
          height: 5,
          borderRadius: "50%",
          background: color[status],
          flex: "none",
        }}
      />
      {label}
    </div>
  );
}

/**
 * Delete every saved lineup — armed by the first click, done by the second.
 *
 * The choice was between a bare ✕ and a labelled button, and the honest answer
 * is that neither is safe on its own: a 20px ✕ beside a count is the easiest
 * thing in the rail to hit by accident, and "CLEAR ALL" is unmistakable but a
 * bigger target for the same accident. So the label wins — it says what it does —
 * and the protection moves to where it belongs: the click that destroys work is
 * never the click you can make by mistake.
 *
 * It disarms itself after 3s, so an accidental first click leaves nothing armed
 * to blunder into later, and a deliberate one needs no cancel button. No modal
 * and no window.confirm: this is a dense keyboard-free data tool, and the arming
 * state is visible in the button itself.
 */
function ClearAll({ count, onConfirm }: { count: number; onConfirm: () => void }) {
  const [armed, setArmed] = useState(false);
  const timer = useRef<number | undefined>(undefined);

  // Cleared on unmount so a pending disarm cannot fire into a dead component
  // (tab switches away from the slate view while armed).
  useEffect(() => () => window.clearTimeout(timer.current), []);

  return (
    <button
      onClick={() => {
        window.clearTimeout(timer.current);
        if (armed) {
          setArmed(false);
          onConfirm();
        } else {
          setArmed(true);
          timer.current = window.setTimeout(() => setArmed(false), 3000);
        }
      }}
      title={
        armed
          ? `Click again to delete all ${count} saved lineups`
          : `Delete all ${count} saved lineups (asks once first)`
      }
      style={{
        border: "none",
        background: "transparent",
        padding: 0,
        fontFamily: font.mono,
        fontSize: 10,
        fontWeight: 600,
        letterSpacing: "0.1em",
        // Amber is rule 3 — a warning about your own state, not a data verdict.
        color: armed ? c.amber : c.dim,
        cursor: "pointer",
      }}
    >
      {armed ? `DELETE ${count}?` : "CLEAR ALL"}
    </button>
  );
}

function Label({ children }: { children: React.ReactNode }) {
  return (
    <div style={{ fontSize: 9, letterSpacing: "0.1em", color: c.dim }}>{children}</div>
  );
}

function Value({ children, color }: { children: React.ReactNode; color?: string }) {
  return (
    <div style={{ fontSize: 15, fontWeight: 600, color: color ?? c.text }}>{children}</div>
  );
}

const primaryBtn: React.CSSProperties = {
  // Blue, not green: Optimize builds the LINEUP, and blue is the lineup.
  background: c.blue,
  color: "#0b0d10",
  padding: 9,
  borderRadius: 4,
  border: "none",
  fontSize: 12,
  fontWeight: 600,
  fontFamily: font.sans,
  cursor: "pointer",
  width: "100%",
};

const secondaryBtn: React.CSSProperties = {
  border: `1px solid ${c.lineStrong}`,
  background: "transparent",
  color: c.text2,
  fontSize: 11.5,
  padding: 7,
  borderRadius: 4,
  fontFamily: font.sans,
  cursor: "pointer",
};
