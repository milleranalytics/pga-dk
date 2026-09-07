import type { CSSProperties, ReactNode } from "react";
import { c, font, radius, type as t, weight } from "../tokens";

/**
 * THE SAVED-LINEUP CARD'S CONTROLS — the name box, and the three buttons
 * beside it.
 *
 * A FILE OF ITS OWN, still, even with ONE caller: these are the pieces the rail
 * is made of and they are worth reading as a unit, away from the 700 lines of
 * panel around them.
 *
 * THE `size` DIAL IS GONE (owner, Sep 2026). It shipped as `"sm" | "lg"` on the
 * assumption that a Lineups tab would follow, the way nfl-dk has one — a second
 * panel drawing the same saved list on wider cards. It will not: golf lineups
 * are six interchangeable names, and comparing two of them is something the
 * rail already does at a glance beside the grid. Football has positional seats,
 * stacks and correlations, which is the reason that tab earns its place THERE
 * and not here.
 *
 * So the second size was a parameter with no second caller — a shape kept ready
 * for a screen nobody is going to build. If one ever is, the dial is four lines
 * and this comment says where they went.
 */
/**
 * THE BUTTON BOX, and it grew from 16 to 18 (owner, Sep 2026: nfl-dk's "are a
 * little bit bigger... making for a larger target").
 *
 * THE BOX AND THE INK ARE TWO DIALS, and only this one is about the target.
 * 18px is a comfortable press on a 278px rail with three controls on it, and it
 * is what the owner asked for. What goes INSIDE it is `CARD_ICON`, and the two
 * were confused once already — see below.
 */
const BTN_DIM = 18;

/**
 * THE DRAWN ICON INSIDE IT. Exported so every caller asks for one size rather
 * than each passing its own and drifting.
 *
 * 12 → 10 (owner, Sep 2026, with the two cards side by side: "make your glyphs
 * a bit smaller so they match the NFL one. your PGA ones are a bit too big").
 * With `INK = 17/24` that is 8.5px of mark → 7.1px, and the strokes thin with
 * it — 1.3px → 1.1px on the cross — because the widths are specified in view-box
 * units and scale with the box.
 *
 * WHY IT WAS 12: A MEASUREMENT TAKEN FROM PROSE. The 12 was derived from
 * nfl-dk's own comment claiming its fallback triangles rendered "~14px of ink"
 * at a 9px font-size, so ours were sized up to meet a 14px neighbour. That
 * number was stale in the file it came from — the same comment goes on to say
 * the face changed and the correction it justified is now `scale(1)` — and it
 * was never re-measured here because the glyphs cannot be measured here: the
 * headless shell has no face for `▲ ▼ ✕` and renders all three as identical
 * tofu boxes. So the one number in this file that was NOT measured is the one
 * that was wrong, and it took the owner putting the two cards next to each
 * other to see it. If these need sizing again, screenshot both — do not read a
 * pixel count out of a comment.
 *
 * The RATIO is what carries over rather than the pixels: nfl-dk sets these
 * glyphs at 9px against a 12px card name; ours are 10px of box against a 13px
 * name, which is the same fraction to within a hair.
 */
export const CARD_ICON = 10;

/**
 * ONE OF THE THREE CONTROLS ON A SAVED CARD — up, down, delete.
 *
 * A real `<button>` rather than the `<span>` the ✕ used to be, so the three of
 * them are reachable by keyboard and `disabled` means something to the browser
 * as well as to the eye. They are the same size whatever they are doing, which
 * is what keeps the name box the same width on every card.
 *
 * A DISABLED ARROW IS DRAWN, DIMMED AND INERT — never removed. The first card
 * has nowhere to move up to, and dropping its button would slide the ✕ two
 * places left on exactly one card in the list, under a cursor that is aiming at
 * the ✕ on all the others.
 *
 * Hover and the disabled colour live in `.cardbtn` (index.css), for the reason
 * every control in this app states: an inline colour cannot be brightened by a
 * `:hover` rule, so the resting colour has to live in the stylesheet.
 *
 * `icon`, NOT `label` (Sep 2026). These were the characters `\u25b2 \u25bc \u2715`, none of
 * which Archivo contains \u2014 so all three arrived from whatever face the system
 * offered, at that face's metrics, and the triangles rendered a different height
 * of ink from the cross beside them. nfl-dk corrected that with a multiplier
 * tuned to one fallback face and the correction went stale when the face
 * changed. These are drawn now (components/icons.tsx), so the three are the same
 * size on every machine by construction and there is nothing left to tune.
 *
 * A DRAWN ICON HAS NO ACCESSIBLE NAME, so `title` stopped being optional: it is
 * the only thing that says what the button does, to a reader and to a check.
 */
export function CardBtn(props: {
  act: string;
  icon: ReactNode;
  disabled: boolean;
  title: string;
  onPress: () => void;
}) {
  return (
    <button
      type="button"
      className="cardbtn"
      data-act={props.act}
      disabled={props.disabled}
      title={props.title}
      // The card loads the lineup on click; these three are about the card, so
      // they stop here. Same reason the name box does.
      onMouseDown={(e) => e.stopPropagation()}
      onClick={(e) => {
        e.stopPropagation();
        props.onPress();
      }}
      style={{
        flex: "0 0 auto",
        width: BTN_DIM,
        height: BTN_DIM,
        padding: 0,
        border: "none",
        background: "transparent",
        // A drawn icon is centred by the box, not by a line-height guessed
        // against a glyph's ink.
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        fontFamily: font.data,
        cursor: props.disabled ? "default" : "pointer",
      }}
    >
      {props.icon}
    </button>
  );
}

/**
 * A SAVED LINEUP'S NAME BOX — ALWAYS A REAL `<input>`, never a span that turns
 * into one on click.
 *
 * A span that swaps to an input reflows the card at the exact moment you are
 * aiming at it, and every card below it shifts by whatever the two elements
 * differ by. So the box is always the input, always at its full size, and it
 * simply does not LOOK like a field until you are over it — `.lineupname` in
 * index.css carries the hover and the placeholder, because an inline style
 * cannot express a pseudo-class.
 *
 * THE BORDER IS ALWAYS DRAWN AND MERELY TRANSPARENT AT REST, for the same
 * reason: an edge that appeared on hover would move the text inside it by a
 * pixel. `minWidth: 0` is what lets a flex child actually shrink — without it
 * the input keeps its default intrinsic width and pushes the three buttons off
 * the end of a 278px rail.
 *
 * SANS, BECAUSE IT IS A NAME. Everything else on the card is either a number —
 * salary, Σ P(TOP-20) — or the list of players underneath. A form control does
 * NOT inherit `font-family`, which is why this has to be said here at all
 * rather than left to `body`.
 *
 * THE NEGATIVE MARGIN IS THE ALIGNMENT, and it is measured rather than guessed.
 * The card pads its content by 10px and the player list starts exactly there;
 * the box insets its own text by its 1px border plus 4px of padding, so -5px
 * puts the FIRST LETTER OF THE NAME on the same vertical as the first letter of
 * the player list below it. The box keeps its padding, so the hover edge has
 * room and simply reaches 5px into the card's gutter — which is also where the
 * extra typing room comes from.
 */
export function nameBoxStyle(): CSSProperties {
  return {
    flex: 1,
    minWidth: 0,
    background: "transparent",
    border: "1px solid transparent",
    borderRadius: radius.sm,
    padding: "1px 4px",
    margin: "0 0 0 -5px",
    fontFamily: font.sans,
    fontSize: t.data,
    fontWeight: weight.semi,
    color: c.text2,
    outline: "none",
  };
}
