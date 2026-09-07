import { c, font, stretch, type as t } from "../tokens";
import type { SyncStatus } from "../persist";
import { shortTime } from "../format";

/**
 * THE SYNC STAMP — a state and a time, riding on the rail's LINEUP heading.
 *
 * IT REPLACED `6 × $50,000`, which was the roster rules restated on a screen
 * that already draws six slots and prints the cap in REMAINING — a permanent
 * line saying something two other things on the same panel already said.
 *
 * WHAT IT SAYS INSTEAD IS THE ONE THING THIS SCREEN COULD NOT TELL YOU: where
 * the build is kept, and when it was last written there. That matters because
 * this app is used on two computers through a OneDrive file, and it changes
 * what a BLANK RAIL MEANS. Opened from disk with the notebook's server not
 * running, an empty rail means "your lineups are on the machine you are not
 * sitting at" — and nothing else on the screen could say so.
 *
 * IT IS NOT SILENT WHEN HEALTHY, and that is a deliberate reversal of what the
 * old badge did (which drew nothing at all unless something was wrong). The
 * reason is that "nothing is wrong" and "nothing is being written" looked
 * identical: both were an empty strip of rail. A stamp that is present in every
 * state is the only version where a missing word means something. It is held at
 * `c.dimmer` — the quietest colour in the palette, quieter than any number
 * beside it — so it is available rather than announced.
 *
 * AMBER ONLY WHEN A WRITE HAS ACTUALLY FAILED. That is rule 3: a caution about
 * the app's own state, never about a player.
 */

/** The line, its hover, and its colour. Separated out because the states read
 *  as sentences and a nested ternary in the markup would not. */
export function describeSync(status: SyncStatus, at?: string): [string, string, string] {
  const stamp = shortTime(at);
  switch (status) {
    case "local":
      return [
        "THIS BROWSER ONLY",
        "Opened straight from disk, so locks, exclusions and saved lineups are kept in " +
          "this browser and nowhere else — they will not be on your other computer.\n\n" +
          "To share them, run the notebook's last cell (serve_dashboard) and open the " +
          "link it prints.",
        c.dim,
      ];
    case "saving":
      return [
        "SAVING…",
        "Writing current.json to the OneDrive folder the notebook's server owns.",
        c.dimmer,
      ];
    case "error":
      return [
        stamp ? `SYNC FAILED · EDITED ${stamp}` : "SYNC FAILED",
        "Could not write current.json — check that the notebook's server window is still " +
          "open. The lineups are safe in this browser meanwhile and will sync on the next " +
          "successful save.\n\n" +
          "The time above is when you last CHANGED something, not when it last reached " +
          "the folder — that is the point of the warning.",
        c.amber,
      ];
    case "idle":
    case "saved":
      return [
        stamp ? `SYNCED · ${stamp}` : "SYNCED",
        "Locks, exclusions and saved lineups are written to the OneDrive folder as you " +
          "work, and OneDrive carries that folder to your other computer.\n\n" +
          (at
            ? `Last written: ${at}`
            : "Nothing written yet this week — the file appears on your first edit."),
        c.dimmer,
      ];
  }
}

export function SyncStamp({
  status,
  at,
  style,
}: {
  status: SyncStatus;
  /**
   * ISO time of the last state change that was written out — `saved_at` on the
   * build state, which is the same field the file carries and the same one
   * `decideSync` arbitrates with, so the stamp cannot disagree with the rule
   * that decided which copy won.
   */
  at?: string;
  style?: React.CSSProperties;
}) {
  const [text, tip, tone] = describeSync(status, at);
  return (
    <div
      data-part="sync"
      data-sync={status}
      title={tip}
      style={{
        fontFamily: font.data,
        fontSize: t.label,
        fontStretch: stretch.label,
        letterSpacing: "0.06em",
        color: tone,
        whiteSpace: "nowrap",
        ...style,
      }}
    >
      {text}
    </div>
  );
}
