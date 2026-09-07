/**
 * THE APP'S GLYPHS, DRAWN RATHER THAN TYPED.
 *
 * WHY THIS FILE EXISTS. Archivo carries no geometric shapes and no Greek, so
 * every `▲ ▼ ▾ ▸ ✕ ✓ Σ ≈` in the source was rendering in WHATEVER FACE the
 * operating system offered for it — a different font, at whatever metrics it
 * came with, inside a page that otherwise has exactly one face.
 *
 * That is not only a look. A fallback face brings its own line box, and an
 * inline run that pulls one in sits at a DIFFERENT BASELINE from the text
 * beside it: on the saved-lineup card, `Σ P20 249.3` sat two pixels below the
 * salary next to it — measured, and the reason the owner reported the two
 * numbers as "not inline" (Sep 2026). The gap depends on which face the machine
 * happens to pick, so it was invisible in a headless check and obvious on
 * Windows.
 *
 * SVG HAS NO SUCH PROBLEM. An inline `<svg>` is a replaced element of exactly
 * the size asked for, aligned by the rules the caller sets, identical on every
 * machine. `stroke="currentColor"` means each one still inherits the colour of
 * whatever control it sits in, so `.cardbtn:hover` and `.lx button[aria-pressed]`
 * keep working untouched.
 *
 * THE ALTERNATIVE WAS TRIED NEXT DOOR AND WENT STALE. nfl-dk kept the glyphs
 * and corrected their size with a multiplier tuned to one fallback face; the
 * face changed, the correction outlived the thing it was correcting, and the
 * arrows ended up MORE wrong than before it was applied. A drawn shape needs no
 * correction to go stale.
 *
 * SIZING: `size` is the box in px. The strokes are specified in a 24-unit view
 * box, so weight scales with the box and a 10px caret is not a hairline.
 */

interface IconProps {
  /** Box size in px. */
  size?: number;
  /** Overrides the inherited colour. Rarely wanted — see `currentColor` above. */
  color?: string;
}

/** Shared: strokes inherit, the glyph is decorative, and the box never shrinks
 *  in a flex row (`flex: none`) or drags the line box around (`block`). */
function svgProps(size: number) {
  return {
    viewBox: "0 0 24 24",
    width: size,
    height: size,
    "aria-hidden": true as const,
    focusable: "false" as const,
    style: { display: "block", flex: "none" } as React.CSSProperties,
  };
}

/** LOCK — a set lock is a green padlock. */
export function LockIcon({ size = 12, color }: IconProps) {
  return (
    <svg {...svgProps(size)} fill="none" stroke={color ?? "currentColor"}
      strokeWidth={2.3} strokeLinecap="round" strokeLinejoin="round">
      <rect x="4" y="10.5" width="16" height="10" rx="2.2" />
      <path d="M8.2 10.5V7.2a3.8 3.8 0 0 1 7.6 0v3.3" />
    </svg>
  );
}

/** EXCLUDE — a slashed circle. Never a letter X: at 10px an `X` in a grid of
 *  numbers was read as a multiplication sign. */
export function BanIcon({ size = 12, color }: IconProps) {
  return (
    <svg {...svgProps(size)} fill="none" stroke={color ?? "currentColor"}
      strokeWidth={2.3} strokeLinecap="round">
      <circle cx="12" cy="12" r="8.6" />
      <path d="M6.1 6.1l11.8 11.8" />
    </svg>
  );
}

/**
 * A SOLID TRIANGLE — the saved card's reorder controls, and the grid's sort
 * indicator. FILLED rather than stroked, because it replaces `▲`/`▼`, which are
 * solid glyphs; a stroked outline at 9px reads as a different symbol entirely.
 *
 * `dir` rather than two components: they are one shape at two rotations, and
 * writing them twice is how the two end up different sizes.
 */
export function Caret({ dir, size = 9, color }: IconProps & { dir: "up" | "down" | "right" }) {
  const rot = { up: 180, down: 0, right: -90 }[dir];
  return (
    <svg {...svgProps(size)} fill={color ?? "currentColor"}>
      <path d="M12 17.5 3.5 7h17z" transform={rot ? `rotate(${rot} 12 12)` : undefined} />
    </svg>
  );
}

/** DELETE / CLOSE. Replaces `✕`, which fell back like the triangles did. */
export function Cross({ size = 10, color }: IconProps) {
  return (
    <svg {...svgProps(size)} fill="none" stroke={color ?? "currentColor"}
      strokeWidth={2.6} strokeLinecap="round">
      <path d="M5 5l14 14M19 5 5 19" />
    </svg>
  );
}

/** A TICK, for a selected item in a list. Replaces `✓`. */
export function Check({ size = 10, color }: IconProps) {
  return (
    <svg {...svgProps(size)} fill="none" stroke={color ?? "currentColor"}
      strokeWidth={3} strokeLinecap="round" strokeLinejoin="round">
      <path d="M4.5 12.5 9.5 18 19.5 6" />
    </svg>
  );
}
