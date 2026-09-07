import type { CSSProperties } from "react";
import { c, font, radius, type as t, weight } from "../tokens";

/**
 * THE SAVED-LINEUP CARD'S CONTROLS — the name box, and the three buttons
 * beside it.
 *
 * A FILE OF ITS OWN because the rail is not the last panel that will draw a
 * saved lineup (a Lineups tab is the obvious next one), and the alternative —
 * a second hand-rolled copy differing only in size — is how the two views drift
 * apart pixel by pixel. `size` is the one dial: `"sm"` is the rail's 278px
 * column, `"lg"` is a wider card.
 */
export type CardSize = "sm" | "lg";

const BTN_DIM: Record<CardSize, number> = { sm: 16, lg: 18 };
const BTN_FONT: Record<CardSize, number> = { sm: 9, lg: 10 };
const NAME_FONT: Record<CardSize, number> = { sm: t.data, lg: t.body };

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
 */
export function CardBtn(props: {
  act: string;
  label: string;
  disabled: boolean;
  title?: string;
  onPress: () => void;
  size: CardSize;
}) {
  const dim = BTN_DIM[props.size];
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
        width: dim,
        height: dim,
        padding: 0,
        border: "none",
        background: "transparent",
        fontSize: BTN_FONT[props.size],
        lineHeight: `${dim}px`,
        fontFamily: font.data,
        cursor: props.disabled ? "default" : "pointer",
      }}
    >
      {props.label}
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
export function nameBoxStyle(size: CardSize): CSSProperties {
  return {
    flex: 1,
    minWidth: 0,
    background: "transparent",
    border: "1px solid transparent",
    borderRadius: radius.sm,
    padding: "1px 4px",
    margin: "0 0 0 -5px",
    fontFamily: font.sans,
    fontSize: NAME_FONT[size],
    fontWeight: weight.semi,
    color: c.text2,
    outline: "none",
  };
}
