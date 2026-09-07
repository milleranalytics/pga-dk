import { useEffect, useRef, useState } from "react";
import { c, font, radius, rowH, stretch, type as t, weight } from "../tokens";
import type { Field } from "../enrich";
import { NAME_MAX, type SavedLineup, type SyncStatus } from "../persist";
import { fmtSalary } from "../format";
import { SyncStamp } from "../components/syncStamp";
import { CardBtn, nameBoxStyle } from "../components/savedCard";

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
  /** ISO time of the last state change — the sync stamp's timestamp. */
  syncedAt?: string;
  /** Set only when the last Gen press came up short; null when it did not. */
  note: string | null;
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
  /** By list position, like every other saved-lineup edit. */
  onRenameSaved: (index: number, name: string) => void;
  /** Reorder. A no-op off either end — see moveSaved in persist.ts. */
  onMoveSaved: (from: number, to: number) => void;
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

  const nameTip =
    "Name this lineup \u2014 what you were thinking when you built it.\\n" +
    "Blank shows the card position. The name and the order travel with the build " +
    "to the other machine.";

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
      {/* THE RAIL'S HEADING SITS ON THE GRID'S HEADER ROW. Both panels start at
          the same y, so LINEUP and the grid's column headings an inch to their
          left read as one row rather than as two that nearly line up.

          `rowH.colHead` AND NOT A PADDING THAT HAPPENS TO WORK OUT: the header's
          height is one statement in tokens.ts, so the two move together.

          `flex: none` IS THE HALF THAT MAKES THE HEIGHT REAL. The rail is a
          COLUMN FLEX with `overflowY: auto`, so every child defaults to
          `flex-shrink: 1` and a declared height is only a starting offer — the
          moment the rail's content overflows, this block would be squeezed to
          its text and the heading would jump. It is the only child this can
          happen to: every other block is sized by its own content, so its
          min-content height IS its height and there is nothing to take. */}
      <div
        style={{
          flex: "none",
          height: rowH.colHead,
          padding: "0 14px",
          boxSizing: "border-box",
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          gap: 8,
        }}
      >
        <div
          style={{
            fontFamily: font.data,
            fontSize: t.colhead,
            fontWeight: weight.semi,
            fontStretch: stretch.label,
            letterSpacing: "0.14em",
            color: c.muted,
          }}
          title={`DraftKings pays ${roster} golfers under a ${fmtSalary(cap)} cap. The cap is spent in REMAINING below.`}
        >
          LINEUP
        </div>
        {/* THE SYNC STATE RIDES ON THE HEADING, in the place "6 × $50,000" used
            to sit. That line restated the roster rules on a panel that already
            draws six slots and prints the cap in REMAINING; this is the one fact
            the screen could not otherwise tell you. See syncStamp.tsx for why it
            speaks in every state rather than only when something is wrong — the
            roster rules survive as this heading's own tooltip. */}
        <SyncStamp status={props.syncStatus} at={props.syncedAt} />
      </div>

      <div style={{ padding: "0 10px" }}>
        {Array.from({ length: roster }, (_, i) => {
          const p = players[i];
          const locked = p ? !!locks[p.id] : false;
          return (
            <div
              key={i}
              data-player-id={p ? p.id : ""}
              data-locked={locked ? "yes" : "no"}
              // A FILLED SLOT IS A BUTTON — clicking it takes the player out —
              // so it brightens on approach like every other control that fires
              // on a press. An EMPTY slot has no handler and gets no class:
              // nothing lights up under a mouse that can do nothing. The wash is
              // a translucent LAYER (`.seatrow` in index.css), not a colour
              // swap, because the resting background is set inline here and a
              // stylesheet rule cannot reach it.
              className={p ? "seatrow" : undefined}
              onClick={p ? () => props.onRemove(p.id) : undefined}
              title={
                p
                  ? locked
                    ? `LOCKED — every solve keeps ${p.PLAYER}. Click to take him out and drop the lock.`
                    : `Click to take ${p.PLAYER} out. The slot stays empty until you Optimize.`
                  : undefined
              }
              style={{
                display: "flex",
                alignItems: "center",
                gap: 9,
                height: rowH.body,
                padding: "0 9px",
                marginBottom: 2,
                borderRadius: radius.sm,
                // Rule 2: a filled slot IS the lineup, so its edge is the blue
                // accent, not grey. LOCK still overrides with green — that is a
                // verdict on the player, which out-ranks plain membership.
                borderLeft: `2px solid ${p ? (locked ? c.green : c.blue) : c.line}`,
                // `backgroundColor`, NEVER the `background` SHORTHAND: the
                // shorthand resets `background-image` to `none` inline, and an
                // inline declaration beats a stylesheet rule — `.seatrow:hover`
                // would match, resolve its token, and paint nothing.
                backgroundColor: p ? c.surface : c.slotEmpty,
                cursor: p ? "pointer" : "default",
              }}
            >
              {p ? (
                <>
                  <div
                    style={{
                      fontFamily: font.sans,
                      fontSize: t.data,
                      fontWeight: weight.semi,
                      flex: 1,
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                      whiteSpace: "nowrap",
                    }}
                  >
                    {p.PLAYER}
                  </div>
                  <div style={{ fontFamily: font.data, fontSize: t.small, color: c.muted }}>
                    {fmtSalary(p.SALARY)}
                  </div>
                  <div
                    style={{
                      fontFamily: font.data,
                      fontSize: t.small,
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
                  <div style={{ fontFamily: font.sans, fontSize: t.data, flex: 1, color: c.axis }}>
                    Empty
                  </div>
                  <div style={{ fontFamily: font.data, fontSize: t.small, color: c.axis }}>—</div>
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
          borderRadius: radius.md,
          background: c.surface,
          display: "grid",
          gridTemplateColumns: "1fr 1fr",
          gap: "5px 10px",
          fontFamily: font.data,
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
        {/* `.optimizebtn` and `.actionbtn` in index.css own the hover on all
            four. Optimize is already at full brightness at rest, so its hover is
            a faint blue WASH rather than a brighten — the same gesture as the
            grey buttons make, tinted to the colour it is already wearing. */}
        <button className="optimizebtn" onClick={props.onOptimize} title={optTip} style={primaryBtn}>
          Optimize
        </button>
        <div style={{ display: "flex", gap: 5 }}>
          <button
            className="actionbtn"
            onClick={props.onGenerate}
            title={genTip}
            style={{ ...secondaryBtn, flex: 1 }}
          >
            Gen {props.genCount}
          </button>
          <button
            className="actionbtn"
            onClick={props.onSave}
            title="Save the six on the rail into SAVED below. A roster already in the list is not added twice."
            style={{ ...secondaryBtn, flex: 1 }}
          >
            Save
          </button>
          <button
            className="actionbtn"
            onClick={props.onClear}
            title="Empty every slot. Locks and exclusions are left alone — use CLR in the grid for those."
            style={{ ...secondaryBtn, width: 30 }}
          >
            ✕
          </button>
        </div>
        {/* Same rule as the sync badge: silent when there is nothing to act on.
            A Gen press that delivers all five says nothing at all. */}
        {props.note && (
          <div
            style={{
              fontFamily: font.data,
              fontSize: t.label,
              lineHeight: 1.5,
              color: c.amber,
            }}
          >
            {props.note}
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
            fontFamily: font.data,
            fontSize: t.colhead,
            fontWeight: weight.semi,
            fontStretch: stretch.label,
            letterSpacing: "0.14em",
            color: c.muted,
          }}
          title={genTip}
        >
          SAVED {saved.length}
        </div>
        {saved.length === 0 ? (
          <div style={{ fontFamily: font.data, fontSize: t.colhead, color: c.axis }}>
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
              // KEYED BY THE PLAYERS, not the position. Keyed by index,
              // deleting a card would make React reuse the deleted one's DOM
              // for its neighbour — and now that the arrows reorder the list,
              // so would moving one. The key travelling WITH the lineup is also
              // what keeps the caret in the name box when the card you are
              // naming moves out from under it.
              key={[...l.ids].sort().join("|")}
              className="savedcard"
              data-saved-index={i}
              data-current={isCurrent ? "yes" : "no"}
              onClick={() => props.onLoadSaved(l)}
              // FOCUSABLE, SO ↑/↓ (OR ←/→) CAN REORDER IT. The name box stops
              // every key from reaching here (its own onKeyDown), which is what
              // keeps this from fighting text-cursor movement while typing a
              // name — only a key pressed on the card itself, or on one of its
              // buttons, arrives here. `moveSaved` is a no-op off either end, so
              // the boundary needs no special case. Escape blurs rather than
              // reordering, so leaving the card is always available.
              tabIndex={0}
              onKeyDown={(e) => {
                if (e.key === "ArrowUp" || e.key === "ArrowLeft") {
                  e.preventDefault();
                  props.onMoveSaved(i, i - 1);
                } else if (e.key === "ArrowDown" || e.key === "ArrowRight") {
                  e.preventDefault();
                  props.onMoveSaved(i, i + 1);
                } else if (e.key === "Escape") {
                  e.currentTarget.blur();
                }
              }}
              title={
                (isCurrent
                  ? "This is the lineup on the rail."
                  : "Load this lineup onto the rail.") +
                "\nFocused, ↑/↓ or ←/→ reorders it."
              }
              style={{
                padding: "8px 10px",
                marginTop: 6,
                borderRadius: radius.md,
                border: `1px solid ${isCurrent ? c.blue : c.lineStrong}`,
                // `backgroundColor`, NEVER the `background` SHORTHAND, and that
                // is what makes the hover work at all: the shorthand resets
                // `background-image` to `none` INLINE, and an inline declaration
                // beats a stylesheet rule — so `.savedcard:hover` would match,
                // resolve its token, and paint nothing.
                backgroundColor: isCurrent ? c.lineupBg : c.surface,
                cursor: "pointer",
              }}
            >
              {/* WHAT YOU CALLED IT, AND THE THREE THINGS YOU CAN DO TO IT.
                  The name takes the whole row because that is the point of it:
                  "SCHEFFLER FADE" does not fit beside two numbers on a 278px
                  rail, and squeezing it in there is how the feature would end up
                  holding four characters. The numbers moved to their own line
                  below, which costs every card about fifteen pixels of height
                  and is what buys the name its room. */}
              <div style={{ display: "flex", alignItems: "center", gap: 3 }}>
                <input
                  data-part="lineup-name"
                  className="lineupname"
                  value={l.name ?? ""}
                  // The card POSITION, which is still the lineup identity —
                  // shown only while it has no name, so an unnamed card reads
                  // exactly as it always did and a named one is not carrying a
                  // number it no longer needs.
                  placeholder={`L${i + 1}`}
                  maxLength={NAME_MAX}
                  spellCheck={false}
                  onChange={(e) => props.onRenameSaved(i, e.target.value)}
                  // THE CARD LOADS THE LINEUP ON CLICK, so every gesture that
                  // belongs to the box has to stop here. `mousedown` as well as
                  // `click`: mousedown is what places the caret, and without it
                  // aiming at the middle of a name would load a lineup.
                  onMouseDown={(e) => e.stopPropagation()}
                  onClick={(e) => e.stopPropagation()}
                  onKeyDown={(e) => {
                    e.stopPropagation();
                    if (e.key === "Enter" || e.key === "Escape") e.currentTarget.blur();
                  }}
                  title={nameTip}
                  style={nameBoxStyle("sm")}
                />
                {/* GREYED AT THE ENDS, NEVER REMOVED: the first card keeps its
                    up arrow, so the three controls sit in the same place on
                    every card and nothing shifts under the cursor as you work
                    down the list. `moveSaved` is a no-op off either end, so a
                    press costs nothing if one gets through. */}
                <CardBtn
                  act="move-up"
                  label="▲"
                  disabled={i === 0}
                  onPress={() => props.onMoveSaved(i, i - 1)}
                  size="sm"
                />
                <CardBtn
                  act="move-down"
                  label="▼"
                  disabled={i === saved.length - 1}
                  onPress={() => props.onMoveSaved(i, i + 1)}
                  size="sm"
                />
                <CardBtn
                  act="delete-saved"
                  label="✕"
                  disabled={false}
                  onPress={() => props.onDeleteSaved(i)}
                  size="sm"
                />
              </div>

              {/* SALARY AND Σ P(TOP-20), BOTH LABELLED. Two numbers side by side
                  of which one is a price and one a probability sum, told apart
                  by a dollar sign alone, is a pair you have to think about; the
                  caption travels inside the same span as its number, so the two
                  stay together if the row wraps. */}
              <div
                style={{
                  display: "flex",
                  gap: 9,
                  marginTop: 4,
                  fontFamily: font.data,
                  fontSize: t.small,
                  color: c.text2,
                }}
              >
                <span data-saved-salary={i}>{fmtSalary(sal)}</span>
                <span data-saved-p20={i}>
                  <span style={{ color: c.dim }}>Σ P20 </span>
                  {(p20 * 100).toFixed(1)}
                </span>
              </div>

              {/* WHO IS IN IT — a run-on sentence of six names, which is what
                  the card was before it had a name box and is still the fastest
                  way to see that two builds differ. */}
              <div
                style={{
                  fontFamily: font.sans,
                  fontSize: t.small,
                  color: c.dim,
                  lineHeight: 1.45,
                  marginTop: 3,
                }}
              >
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
      // `.quietbtn` owns the resting grey and the hover; the ARMED amber below
      // is inline, which beats it — an armed button must not dim back down
      // under the very mouse that is about to press it again.
      className="quietbtn"
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
        fontFamily: font.data,
        fontSize: t.colhead,
        fontWeight: weight.semi,
        fontStretch: stretch.label,
        letterSpacing: "0.1em",
        // Amber is rule 3 — a warning about your own state, not a data verdict.
        color: armed ? c.amber : undefined,
        cursor: "pointer",
      }}
    >
      {armed ? `DELETE ${count}?` : "CLEAR ALL"}
    </button>
  );
}

function Label({ children }: { children: React.ReactNode }) {
  return (
    <div
      style={{
        fontSize: t.micro,
        fontStretch: stretch.label,
        letterSpacing: "0.1em",
        color: c.dim,
      }}
    >
      {children}
    </div>
  );
}

function Value({ children, color }: { children: React.ReactNode; color?: string }) {
  return (
    <div style={{ fontSize: t.lead, fontWeight: weight.semi, color: color ?? c.text }}>
      {children}
    </div>
  );
}

const primaryBtn: React.CSSProperties = {
  // Blue, not green: Optimize builds the LINEUP, and blue is the lineup.
  //
  // Outlined, not filled. Once blue became the actual Windows accent it got
  // bright enough that a filled slab was the loudest thing on the screen — a
  // permanent maximum, sitting next to a grid whose whole job is to let a
  // number stand out. It is the same borrowed treatment as an in-lineup slot
  // or the active saved card: blue edge, dark interior. Weight and a blue
  // label still rank it above Gen/Save/✕, which are grey on grey.
  //
  // Filled blue is reserved for STATE that is currently on (CardBtn's active
  // "In lineup"), where the fill is the message. An action button is not a
  // state, so it does not get the fill.
  background: "transparent",
  border: `1px solid ${c.blue}`,
  color: c.blue,
  padding: 9,
  borderRadius: radius.md,
  fontSize: t.data,
  fontWeight: weight.semi,
  fontFamily: font.sans,
  cursor: "pointer",
  width: "100%",
};

const secondaryBtn: React.CSSProperties = {
  // THE BORDER AND THE COLOUR ARE `.actionbtn`'s, in index.css, and are
  // deliberately NOT set here: an inline colour cannot be brightened by a
  // `:hover` rule, so a resting colour written inline would be a button that
  // never acknowledges the cursor. Only the geometry lives inline.
  border: "1px solid",
  fontSize: t.small,
  padding: 7,
  borderRadius: radius.md,
  fontFamily: font.sans,
  cursor: "pointer",
};
