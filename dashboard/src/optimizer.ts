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

  // Full roster, any spend at or under the cap.
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

/** Deterministic 0..1 hash, so successive Gen runs diverge reproducibly. */
function jitter(id: string, seed: number): number {
  let h = 2166136261 ^ seed;
  for (let i = 0; i < id.length; i++) {
    h ^= id.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return ((h >>> 0) % 10000) / 10000;
}

const USAGE_PENALTY = 0.006;
const JITTER_SCALE = 0.004;

/**
 * N distinct optimal-under-constraints lineups.
 *
 * The hard exposure ceiling matters and is not decoration: an earlier
 * prototype version used only the soft penalty, which does not actually bound
 * exposure and quietly lets one player appear in every lineup.
 */
export function generate(
  ctx: BuildContext,
  n: number,
  existing: string[][],
  maxExposurePct: number,
): string[][] {
  const out: string[][] = [];
  const seen = new Set(existing.map((ids) => [...ids].sort().join("|")));
  const usage = new Map<string, number>();
  for (const ids of existing) {
    for (const id of ids) usage.set(id, (usage.get(id) ?? 0) + 1);
  }

  const total = existing.length + n;
  const ceiling = Math.max(1, Math.floor((maxExposurePct / 100) * total));

  let attempts = 0;
  const maxAttempts = 8 * n;

  while (out.length < n && attempts < maxAttempts) {
    attempts++;

    // Ban anyone at the ceiling — except locks, which are exempt by definition.
    const banned = new Set<string>();
    for (const [id, u] of usage) {
      if (u >= ceiling && !ctx.lockedIds.has(id)) banned.add(id);
    }

    const seed = attempts;
    const penalized = ctx.all.map((p) => ({
      ...p,
      value:
        p.value -
        USAGE_PENALTY * (usage.get(p.id) ?? 0) -
        JITTER_SCALE * jitter(p.id, seed),
    }));

    const lineup = optimize({
      ...ctx,
      all: penalized,
      excludedIds: new Set([...ctx.excludedIds, ...banned]),
    });
    if (!lineup) continue;

    const ids = lineup.map((p) => p.id);
    const key = [...ids].sort().join("|");
    if (seen.has(key)) continue;

    seen.add(key);
    out.push(ids);
    for (const id of ids) usage.set(id, (usage.get(id) ?? 0) + 1);
  }

  return out;
}
