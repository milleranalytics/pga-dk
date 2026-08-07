/**
 * Exact 0/1 knapsack with a cardinality constraint — the DraftKings lineup.
 *
 * INVARIANT (must hold by definition, not by inspection):
 *   1. The returned lineup contains exactly `slots` players, ALL DISTINCT.
 *   2. Its total salary is <= cap.
 *   3. Its objective equals the exhaustive maximum over all feasible lineups.
 *
 * Verified against brute force in test/optimizer-check.mjs. Do not change this
 * file without re-running that.
 *
 * ---------------------------------------------------------------------------
 * Why this is not the prototype's implementation
 *
 * The design prototype (design/PGA Slate Terminal.dc.html:496) collapses the
 * item dimension and reconstructs from a single `choice[s][b]` = "last player
 * to improve this state". Its forward pass is correct, but the reconstruction
 * is not: a player updates EVERY slot level during its own iteration, so the
 * same player can own both choice[s][b] and choice[s-1][b-w], and the walk-back
 * emits it twice. Measured rate: 34% of solves (test/dp-check.mjs). It is
 * visible in the prototype's own screenshots — a lineup rostering Rickie Fowler
 * in two slots.
 *
 * The fix is to keep the item dimension EXPLICIT. dp[i][s][b] is the best
 * objective using only the first i players, exactly s slots, exactly b salary
 * buckets. Then "was player i-1 taken at this state?" is answerable directly —
 * the value changed between layer i-1 and layer i — and no player can be
 * selected twice because the walk visits each layer once.
 *
 * Cost: (n+1) x (slots+1) x (buckets+1) float64s. At 149 players / 6 slots /
 * 500 buckets that is ~4 MB and ~500k operations — about a millisecond. There
 * is no reason to be clever here, and being clever is what broke it.
 */

const NEG = -Infinity;

export interface OptPlayer {
  id: string;
  salary: number;
  /** Objective contribution. P_TOP20 normally; MODEL_SCORE when switched. */
  value: number;
}

/**
 * Maximize summed value over exactly `slots` distinct players from `pool`,
 * subject to total salary <= cap. Returns null when infeasible.
 */
export function solve(
  pool: OptPlayer[],
  slots: number,
  cap: number,
): OptPlayer[] | null {
  if (slots === 0) return cap >= 0 ? [] : null;
  if (slots < 0 || cap < 0 || pool.length < slots) return null;

  // Salaries are always multiples of 100, so the salary axis discretizes.
  const B = Math.floor(cap / 100);
  const n = pool.length;
  const S = slots + 1;
  const W = B + 1;
  const layer = S * W;

  const weights = pool.map((p) => Math.floor(p.salary / 100));

  const dp = new Float64Array((n + 1) * layer).fill(NEG);
  dp[0] = 0; // layer 0, s=0, b=0: empty lineup, nothing spent

  for (let i = 1; i <= n; i++) {
    const base = i * layer;
    const prevBase = (i - 1) * layer;
    const w = weights[i - 1];
    const v = pool[i - 1].value;

    for (let s = 0; s < S; s++) {
      const row = base + s * W;
      const prevRow = prevBase + s * W;
      const skipRow = prevBase + (s - 1) * W;

      for (let b = 0; b < W; b++) {
        // Skip player i-1: inherit the previous layer unchanged.
        let best = dp[prevRow + b];

        // Take player i-1: only legal if there is a slot to spend and the
        // salary fits. Reads layer i-1, so player i-1 cannot already be in it.
        if (s > 0 && b >= w) {
          const prev = dp[skipRow + (b - w)];
          if (prev !== NEG) {
            const cand = prev + v;
            if (cand > best) best = cand;
          }
        }
        dp[row + b] = best;
      }
    }
  }

  // Full roster, any spend AT OR UNDER the cap — the cap is a ceiling, never a
  // target. Scanning b upward and keeping a bucket only on a STRICT improvement
  // makes the cheapest of equally-valued lineups win, so leftover salary is
  // returned rather than spent on nothing. (Asserted against brute force in
  // test/optimizer-check.ts. On a real slate the answer still spends the whole
  // cap, because there more salary does buy more P(top-20) — that is the field
  // being priced sensibly, not the solver padding the bill.)
  let bestVal = NEG;
  let bestB = -1;
  const finalRow = n * layer + slots * W;
  for (let b = 0; b < W; b++) {
    if (dp[finalRow + b] > bestVal) {
      bestVal = dp[finalRow + b];
      bestB = b;
    }
  }
  if (bestB < 0 || bestVal === NEG) return null;

  // Walk back one layer at a time. Each layer is visited exactly once, so a
  // player can be selected at most once — the invariant is structural here
  // rather than something the reconstruction has to be careful about.
  const picked: OptPlayer[] = [];
  let s = slots;
  let b = bestB;
  for (let i = n; i >= 1 && s > 0; i--) {
    const here = dp[i * layer + s * W + b];
    const skipped = dp[(i - 1) * layer + s * W + b];
    if (here !== skipped) {
      picked.push(pool[i - 1]);
      b -= weights[i - 1];
      s -= 1;
    }
  }

  return picked.length === slots ? picked.reverse() : null;
}

// ---------------------------------------------------------------------------

export interface BuildContext {
  /** Every player available this week, already enriched with the objective. */
  all: OptPlayer[];
  /** Forced into every solve. */
  lockedIds: Set<string>;
  /** Removed from every solve. */
  excludedIds: Set<string>;
  /** Already in the current build; pre-committed like locks. */
  pickedIds: string[];
  slots: number;
  cap: number;
}

/**
 * Fill the current build to a full roster, keeping locks and manual picks.
 * Pre-committed players have their salary removed from the cap and their slots
 * from the count before the DP runs, so the search space stays small.
 */
/**
 * Why `optimize` returned null — one sentence naming the constraint that ran out.
 *
 * It exists because "no lineup" used to be reported by doing nothing at all:
 * App ran `const r = optimize(ctx); if (!r) return;`, so a roster locked seven
 * deep answered a button press with silence. Gen has had a shortfall message
 * since it was written; Optimize now does too, and the two read alike.
 *
 * The reasons are tested in the SAME ORDER `optimize` rejects in, so the
 * sentence always names the first thing that actually failed rather than a
 * later one that also happens to be true. Only call this when optimize returned
 * null: the final branch is a residual ("nothing else explains it"), and on a
 * feasible context it would be a false statement.
 */
export function whyInfeasible(ctx: BuildContext): string {
  const byId = new Map(ctx.all.map((p) => [p.id, p]));
  const must = [...new Set<string>([...ctx.lockedIds, ...ctx.pickedIds])]
    .map((id) => byId.get(id))
    .filter(Boolean) as OptPlayer[];
  const money = (n: number) => `$${n.toLocaleString("en-US")}`;

  if (must.length > ctx.slots) {
    const over = must.length - ctx.slots;
    return `${must.length} locks for ${ctx.slots} slots — unlock ${over} to make room.`;
  }
  const cost = must.reduce((a, p) => a + p.salary, 0);
  if (cost > ctx.cap) {
    return `your ${must.length} locks cost ${money(cost)} of a ${money(ctx.cap)} cap.`;
  }
  // Enough slots and enough money for the locks themselves, so what failed is
  // filling what is left out of a pool the exclusions have thinned.
  const mustIds = new Set(must.map((m) => m.id));
  const available = ctx.all.filter(
    (p) => !ctx.excludedIds.has(p.id) && !mustIds.has(p.id),
  ).length;
  const need = ctx.slots - must.length;
  if (available < need) {
    return `only ${available} players left to fill ${need} slots — too many exclusions.`;
  }
  return `no roster fits ${money(ctx.cap - cost)} across the remaining ${need} slots under the current locks and exclusions.`;
}

export function optimize(ctx: BuildContext): OptPlayer[] | null {
  const byId = new Map(ctx.all.map((p) => [p.id, p]));
  const mustIds = new Set<string>([...ctx.lockedIds, ...ctx.pickedIds]);
  const must = [...mustIds].map((id) => byId.get(id)).filter(Boolean) as OptPlayer[];

  if (must.length > ctx.slots) return null;
  const remCap = ctx.cap - must.reduce((a, p) => a + p.salary, 0);
  const remSlots = ctx.slots - must.length;
  if (remCap < 0) return null;
  if (remSlots === 0) return must;

  const pool = ctx.all.filter(
    (p) => !mustIds.has(p.id) && !ctx.excludedIds.has(p.id),
  );
  const rest = solve(pool, remSlots, remCap);
  return rest ? [...must, ...rest] : null;
}

/**
 * Per prior appearance, subtracted from a player's objective while generating.
 *
 * Purely a spreading nudge, and safe to tune: distinctness is now guaranteed by
 * the search (see nextDistinct), not by this penalty. Sized against the field —
 * P_TOP20 gaps between the best and second-best lineup run ~0.005-0.02 — so one
 * prior appearance is roughly one such gap.
 */
const USAGE_PENALTY = 0.006;

/** Safety valve on the enumeration below. Never reached on a normal slate. */
const MAX_SOLVES_PER_LINEUP = 400;

/** Why a Gen run returned fewer lineups than asked for. */
export type GenStop =
  | "complete"
  /** No lineup fits the cap at all under the current locks/exclusions. */
  | "infeasible"
  /** Every lineup left would push someone past the exposure ceiling. */
  | "exposure"
  /** The constrained problem has no further DISTINCT lineups. */
  | "exhausted"
  /** MAX_SOLVES_PER_LINEUP hit — a bound, not a real limit of the problem. */
  | "capped";

export interface GenResult {
  lineups: string[][];
  stop: GenStop;
  /** Appearances allowed per player across existing + generated. */
  ceiling: number;
}

/**
 * N distinct lineups, best-first, honouring locks, exclusions and a hard
 * exposure ceiling.
 *
 * INVARIANT: returns exactly `n` lineups whenever `n` lineups exist that are
 * distinct from each other and from `existing` and satisfy those constraints.
 * When it returns fewer, `stop` says which constraint ran out — the caller is
 * expected to show that, because "Gen 5 added 3" is otherwise unexplainable.
 *
 * The previous implementation could not hold that invariant and in practice
 * returned ONE lineup on an empty saved list. It re-solved a jittered objective
 * up to 8n times and dropped any duplicate result — but a duplicate changed no
 * state (usage only counted ACCEPTED lineups, and the jitter was an order of
 * magnitude smaller than the gap between the best and second-best lineup), so
 * every remaining attempt re-derived the same optimum and threw it away.
 *
 * The fix is to stop hoping a perturbed objective lands somewhere new and to
 * make "best lineup that is not one of these" a thing the search itself
 * answers — Lawler's partitioning, in nextDistinct. Two layers, each doing the
 * job it is actually able to do:
 *
 *   - the exposure ceiling BANS players from the pool, at search-space level,
 *     which is cheap and exact;
 *   - distinctness is a branch-and-bound over exclusions, which terminates in
 *     a distinct lineup or in proof that none exists.
 *
 * The usage penalty survives as what it always was — a nudge to spread the
 * field rather than stack the same core until it hits the ceiling. It can no
 * longer cause the stall above: the search is what guarantees progress.
 */
export function generate(
  ctx: BuildContext,
  n: number,
  existing: string[][],
  maxExposurePct: number,
): GenResult {
  // Gen solves fresh rosters; only locks are pre-committed. Stated here rather
  // than left to the caller because nextDistinct's branching depends on it —
  // see the `free` filter there.
  const base: BuildContext = { ...ctx, pickedIds: [] };

  const key = (ids: string[]) => [...ids].sort().join("|");
  const seen = new Set(existing.map(key));
  const usage = new Map<string, number>();
  for (const ids of existing) {
    for (const id of ids) usage.set(id, (usage.get(id) ?? 0) + 1);
  }

  const ceiling = Math.max(1, Math.floor((maxExposurePct / 100) * (existing.length + n)));

  const lineups: string[][] = [];
  let stop: GenStop = "complete";

  while (lineups.length < n) {
    // Ban anyone at the ceiling — except locks, which are exempt by definition
    // (asking for a player in every lineup and capping their exposure are
    // contradictory instructions, and the explicit one wins).
    const banned = new Set<string>();
    for (const [id, u] of usage) {
      if (u >= ceiling && !base.lockedIds.has(id)) banned.add(id);
    }

    const found = nextDistinct(base, banned, usage, seen);
    if (!found.ids) {
      // Which constraint ran out? The banned search cannot tell "no roster
      // fits" apart from "no roster fits WITHOUT these players", so re-ask
      // without the exposure bans and let that answer speak: a lineup there
      // means exposure is what stopped us, and no lineup there means the
      // shortfall was never about exposure at all.
      if (found.reason === "capped" || banned.size === 0) stop = found.reason;
      else {
        const unbanned = nextDistinct(base, new Set(), usage, seen);
        stop = unbanned.ids ? "exposure" : unbanned.reason;
      }
      break;
    }

    seen.add(key(found.ids));
    lineups.push(found.ids);
    for (const id of found.ids) usage.set(id, (usage.get(id) ?? 0) + 1);
  }

  return { lineups, stop, ceiling };
}

/**
 * The best lineup that is not already in `seen`, or why there is none.
 *
 * Lawler's partitioning. A node is (E, F): players excluded from it, players
 * forced into it. Solving a node gives that node's best lineup L. If L is
 * already seen, the node's remaining lineups — every feasible lineup in (E, F)
 * except L — partition exactly into one child per free player of L:
 *
 *     child j = (E ∪ {L_j},  F ∪ {L_1 … L_j-1})
 *
 * i.e. "keep the first j-1 of L, drop L_j". Disjoint by construction (they
 * disagree on which prefix of L they keep) and jointly exhaustive (any lineup
 * ≠ L must drop some first member of L). So expanding the highest-valued node
 * each time walks lineups in descending order without ever revisiting one, and
 * an empty queue is proof that no distinct lineup exists.
 *
 * Values are penalized by usage, so "descending" means descending under the
 * spreading objective, not raw P_TOP20. That is the intended ordering.
 */
function nextDistinct(
  ctx: BuildContext,
  banned: Set<string>,
  usage: Map<string, number>,
  seen: Set<string>,
): { ids: string[] | null; reason: Extract<GenStop, "infeasible" | "exhausted" | "capped"> } {
  const penalized = ctx.all.map((p) => ({
    ...p,
    value: p.value - USAGE_PENALTY * (usage.get(p.id) ?? 0),
  }));
  const valueOf = new Map(penalized.map((p) => [p.id, p.value]));

  interface Node {
    ex: string[];
    fo: string[];
    ids: string[];
    val: number;
  }

  const solveNode = (ex: string[], fo: string[]): Node | null => {
    const r = optimize({
      ...ctx,
      all: penalized,
      pickedIds: fo,
      excludedIds: new Set([...ctx.excludedIds, ...banned, ...ex]),
    });
    if (!r) return null;
    const ids = r.map((p) => p.id);
    return { ex, fo, ids, val: ids.reduce((a, id) => a + (valueOf.get(id) ?? 0), 0) };
  };

  let solves = 1;
  const root = solveNode([], []);
  if (!root) return { ids: null, reason: "infeasible" };

  const queue: Node[] = [root];
  while (queue.length) {
    // Small queue (one entry per branch of one 6-slot lineup, a few dozen at
    // worst), so a sort beats maintaining a heap.
    queue.sort((a, b) => b.val - a.val);
    const node = queue.shift() as Node;
    if (!seen.has([...node.ids].sort().join("|"))) return { ids: node.ids, reason: "exhausted" };

    // Branch only on players this node CHOSE. A pre-committed player — locked
    // by the user, or forced by an ancestor — is in `must` inside optimize()
    // and therefore immune to exclusion: branching on one would re-solve to
    // the identical lineup and loop.
    const free = node.ids.filter((id) => !node.fo.includes(id) && !ctx.lockedIds.has(id));
    for (let j = 0; j < free.length; j++) {
      if (solves >= MAX_SOLVES_PER_LINEUP) return { ids: null, reason: "capped" };
      const child = solveNode([...node.ex, free[j]], [...node.fo, ...free.slice(0, j)]);
      solves++;
      if (child) queue.push(child);
    }
  }
  return { ids: null, reason: "exhausted" };
}
