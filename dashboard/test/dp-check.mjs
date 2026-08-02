// Does the prototype's solve() reconstruction ever emit the same player twice,
// and does it return the true optimum? Brute force is the oracle.

// --- the prototype's algorithm, transcribed verbatim from
// --- "PGA Slate Terminal.dc.html" lines 496-528 (no must/banned/penalty) ---
function protoSolve(pool, K, cap) {
  const B = Math.floor(cap / 100);
  const N = (K + 1) * (B + 1);
  const dp = new Float64Array(N).fill(-1);
  const ch = new Int32Array(N).fill(-1);
  dp[0] = 0;
  for (let pi = 0; pi < pool.length; pi++) {
    const p = pool[pi], w = Math.floor(p.SALARY / 100), v = p.val;
    for (let s = K; s >= 1; s--) {
      for (let b = B; b >= w; b--) {
        const prev = dp[(s - 1) * (B + 1) + (b - w)];
        if (prev < 0) continue;
        const cand = prev + v;
        if (cand > dp[s * (B + 1) + b]) { dp[s * (B + 1) + b] = cand; ch[s * (B + 1) + b] = pi; }
      }
    }
  }
  let best = -1, bb = -1;
  for (let b = 0; b <= B; b++) { const v = dp[K * (B + 1) + b]; if (v > best) { best = v; bb = b; } }
  if (bb < 0) return null;
  const out = [];
  let s = K, b = bb;
  while (s > 0) {
    const pi = ch[s * (B + 1) + b]; if (pi < 0) break;
    const p = pool[pi]; out.push(p); b -= Math.floor(p.SALARY / 100); s--;
  }
  return { picks: out, dpValue: best };
}

// --- oracle: exhaustive over all K-subsets ---
function brute(pool, K, cap) {
  let best = -1, bestSet = null;
  const idx = [];
  (function rec(start, chosen, sal, val) {
    if (chosen === K) { if (sal <= cap && val > best) { best = val; bestSet = idx.slice(); } return; }
    for (let i = start; i < pool.length; i++) {
      if (sal + pool[i].SALARY > cap) continue;
      idx.push(i); rec(i + 1, chosen + 1, sal + pool[i].SALARY, val + pool[i].val); idx.pop();
    }
  })(0, 0, 0, 0);
  return { value: best, set: bestSet };
}

let dupes = 0, subopt = 0, trials = 3000;
let firstDupe = null;
for (let t = 0; t < trials; t++) {
  const n = 8 + Math.floor(Math.random() * 5);
  const pool = Array.from({ length: n }, (_, i) => ({
    id: i,
    SALARY: (60 + Math.floor(Math.random() * 46)) * 100,   // $6,000-$10,500
    val: Math.round(Math.random() * 5000) / 10000,
  }));
  const K = 6, cap = 50000;
  const got = protoSolve(pool, K, cap);
  const want = brute(pool, K, cap);
  if (!got || want.value < 0) continue;

  const ids = got.picks.map(p => p.id);
  const unique = new Set(ids).size === ids.length;
  if (!unique) {
    dupes++;
    if (!firstDupe) firstDupe = { pool, ids, dpValue: got.dpValue, brute: want.value };
  }
  // dp VALUE should still equal the true optimum even when reconstruction breaks
  if (Math.abs(got.dpValue - want.value) > 1e-9) subopt++;
}

console.log(`trials with a feasible lineup: ${trials}`);
console.log(`reconstructions containing a DUPLICATE player: ${dupes}`);
console.log(`dp objective != brute-force optimum:            ${subopt}`);
if (firstDupe) {
  console.log(`\nfirst failing case — picked ids: [${firstDupe.ids}]`);
  console.log(`  dp value ${firstDupe.dpValue.toFixed(4)} vs true optimum ${firstDupe.brute.toFixed(4)}`);
  const salaries = firstDupe.ids.map(i => firstDupe.pool[i].SALARY);
  console.log(`  salaries: ${salaries.join(", ")}  total $${salaries.reduce((a, b) => a + b, 0)}`);
}
