/**
 * Number formatting. Centralised because the design is strict about glyphs:
 * negatives use U+2212 MINUS SIGN, not a hyphen, so columns of signed numbers
 * align and read as arithmetic rather than as hyphenated text.
 */

export const MINUS = "−";
export const EM_DASH = "—";

/** Always-signed, with a true minus sign. "+1.22" / "−0.41". */
export function fmtSigned(v: number, digits: number): string {
  const s = Math.abs(v).toFixed(digits);
  if (v > 0) return `+${s}`;
  if (v < 0) return `${MINUS}${s}`;
  return `+${s}`;
}

/** Signed for display in dense grid cells, where 0 should read as 0.00. */
export function fmtDelta(v: number, digits: number): string {
  const s = Math.abs(v).toFixed(digits);
  return v < 0 ? `${MINUS}${s}` : s;
}

export function fmtSalary(v: number): string {
  return `$${v.toLocaleString("en-US")}`;
}

/** Fractional outright odds: 11 renders as "11/1". */
export function fmtOdds(v: number): string {
  return Number.isFinite(v) ? `${v.toFixed(0)}/1` : EM_DASH;
}

/**
 * An ISO timestamp as a local wall clock, "14:32" — the sync stamp's half of a
 * date, and nothing more.
 *
 * A DATE IS NOT SHOWN, deliberately: the build state is keyed on the week, so
 * anything this stamp can be describing was written during the week you are
 * looking at, and the only question a sync line is ever asked is "did my last
 * few minutes of work land". Returns "" for a missing or unparseable value so
 * the caller can drop the segment entirely rather than print "Invalid Date".
 */
export function shortTime(iso: string | undefined): string {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit", hour12: false });
}

export function fmtDate(iso: string): string {
  const d = new Date(iso + "T00:00:00");
  if (Number.isNaN(d.getTime())) return iso;
  const mon = d.toLocaleString("en-US", { month: "short" });
  return `${mon} ${d.getDate()} '${String(d.getFullYear()).slice(2)}`;
}
