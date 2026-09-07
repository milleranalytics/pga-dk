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
 *
 * EVERY ICON HERE CARRIES THE SAME HEIGHT OF INK AT THE SAME `size` — `INK`
 * below — and that is the rule the owner asked for (Sep 2026: "look at the
 * exact arrows and Xs that NFL uses for lineup cards. they have the same
 * vertical height where the ones you chose do not").
 *
 * WHY IT NEEDS STATING. A `size` prop sets the BOX, and a box is not what the
 * eye measures; it measures the marks inside it. The saved card's ▲ ▼ ✕ are
 * all `size={CARD_ICON}` and looked wrong side by side because the triangle
 * only filled 10.5 of its 24 units while the cross filled 16.6 — same box,
 * ink 58% different, so the arrows read as the small buttons next to a big one.
 * Without this rule `size` is a promise the shapes do not keep.
 *
 * nfl-dk enforces exactly this on its three glyph buttons (`.cardbtn`, and
 * `check_saved_ui.py:V9b` reads the three ink boxes off the rendered card and
 * requires them equal to within a pixel). It has to, because it is correcting a
 * FALLBACK FACE it does not control. We draw the shapes, so we can simply build
 * them to the rule — but the same guard is worth having, because the numbers
 * below are hand-derived and the next shape added here will be too. See
 * `icons.mjs`, which measures the rendered ink of every icon in the app.
 */

/**
 * THE INK HEIGHT EVERY ICON FILLS, in view-box units out of 24 — so an icon at
 * `size={12}` puts 8.5px of mark on screen whichever one it is.
 *
 * 17 rather than 24 because an icon that filled its box edge to edge would sit
 * tighter to its neighbours than the type around it does; 17/24 leaves the same
 * kind of shoulder a cap-height glyph has inside its em box.
 *
 * A STROKED SHAPE'S INK INCLUDES ITS STROKE, and half of it hangs outside the
 * path on each side, so each drawing below solves `path extent + strokeWidth =
 * INK` rather than setting the path extent to INK. That is the step that is
 * easy to skip and is why the cross and the caret drifted apart in the first
 * place: the caret is FILLED, so for it alone path extent and ink are the same
 * number.
 */
const INK = 17;
const HALF = INK / 2; // 8.5 — the ink runs from 12-HALF to 12+HALF

/** Every path above is derived from `INK`; this is where that is asserted in
 *  code rather than only in prose, and it is what `icons.mjs` measures. */
export const ICON_INK_RATIO = INK / 24;
export const ICON_INK_HALF = HALF;

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

/** LOCK — a set lock is a green padlock.
 *
 *  INK: stroke 2.3, so the drawing spans 4.6 → 19.3 (14.7) and the stroke adds
 *  1.15 at each end: 3.45 → 20.45, which is 17. The shackle's arc tops out at
 *  7.87 − 3.27 = 4.6, which is what sets the top. */
export function LockIcon({ size = 12, color }: IconProps) {
  return (
    <svg {...svgProps(size)} fill="none" stroke={color ?? "currentColor"}
      strokeWidth={2.3} strokeLinecap="round" strokeLinejoin="round">
      <rect x="5.1" y="10.7" width="13.8" height="8.6" rx="1.9" />
      <path d="M8.73 10.7V7.87a3.27 3.27 0 0 1 6.54 0v2.83" />
    </svg>
  );
}

/** EXCLUDE — a slashed circle. Never a letter X: at 10px an `X` in a grid of
 *  numbers was read as a multiplication sign. */
export function BanIcon({ size = 12, color }: IconProps) {
  return (
    <svg {...svgProps(size)} fill="none" stroke={color ?? "currentColor"}
      strokeWidth={2.3} strokeLinecap="round">
      {/* INK: 2r + stroke = 17, so r = 7.35. The slash stops on the circle
          rather than crossing it — its ends sit at r/√2 = 5.2 from centre —
          so the stroke's round caps do not stick out past the rim and make the
          icon taller on the diagonal than it is on the axis. */}
      <circle cx="12" cy="12" r="7.35" />
      <path d="M6.8 6.8l10.4 10.4" />
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
 *
 * INK: it is FILLED, so there is no stroke to account for and the triangle is
 * simply INK tall — 3.5 → 20.5. It was 10.5 tall, which is the whole of what
 * the owner was looking at on the saved card.
 *
 * AS WIDE AS IT IS TALL, which is `▲`'s own proportion and not the squat 17×10.5
 * this was. Rotating a shape that is wider than it is tall would also make the
 * `right` caret a DIFFERENT height from the other two, so equal width and
 * height is what makes one path safe to use at three rotations.
 */
export function Caret({ dir, size = 9, color }: IconProps & { dir: "up" | "down" | "right" }) {
  const rot = { up: 180, down: 0, right: -90 }[dir];
  return (
    <svg {...svgProps(size)} fill={color ?? "currentColor"}>
      <path d="M12 20.5 3.5 3.5h17z" transform={rot ? `rotate(${rot} 12 12)` : undefined} />
    </svg>
  );
}

/** DELETE / CLOSE. Replaces `✕`, which fell back like the triangles did.
 *
 *  INK: stroke 2.6, so the arms span 14.4 (4.8 → 19.2) and the caps carry it to
 *  17. Barely moved — this one was already close, and it is the caret that came
 *  up to meet it. */
export function Cross({ size = 10, color }: IconProps) {
  return (
    <svg {...svgProps(size)} fill="none" stroke={color ?? "currentColor"}
      strokeWidth={2.6} strokeLinecap="round">
      <path d="M4.8 4.8l14.4 14.4M19.2 4.8 4.8 19.2" />
    </svg>
  );
}

/** A TICK, for a selected item in a list. Replaces `✓`.
 *
 *  INK: stroke 3, so the drawing spans 14 (5 → 19) and the caps carry it to 17. */
export function Check({ size = 10, color }: IconProps) {
  return (
    <svg {...svgProps(size)} fill="none" stroke={color ?? "currentColor"}
      strokeWidth={3} strokeLinecap="round" strokeLinejoin="round">
      <path d="M4.5 12.58 9.5 19 19.5 5" />
    </svg>
  );
}
