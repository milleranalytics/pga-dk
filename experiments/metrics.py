# experiments/metrics.py
# The forward tests' per-event yardstick, shared by experiments/sg_form_eval.py
# and pga_api.validate: hits@15 (actual top-20s among the top 15 by score),
# per-event AUC for top 20, Spearman against finish, and Brier for a probability.
# From experiments/forward_eval.py, the July 2026 golf.db evaluation (now in
# old_workflow/), so every result since is on the same scale.

import numpy as np
from scipy.stats import spearmanr
from sklearn.metrics import roc_auc_score


def score_event(test_df, score, label_col="TOP_20", is_prob=None):
    """Per-event metrics for a score where higher = better player."""
    y = test_df[label_col].to_numpy()
    score = np.asarray(score, dtype=float)
    n_pos = int(y.sum())
    # Fair tie-breaking: rows arrive in finish order (tournaments table is
    # stored by POS), so a plain argsort resolves tied scores toward the
    # actual result — inflating hits@15 for tie-heavy scores like raw odds.
    # Average hits over random permutations instead.
    rng = np.random.default_rng(len(y) * 7919 + n_pos)
    hits = []
    for _ in range(20):
        perm = rng.permutation(len(score))
        order = perm[np.argsort(-score[perm], kind="stable")]
        hits.append(int(y[order[:15]].sum()))
    hits15 = float(np.mean(hits))
    auc = roc_auc_score(y, score) if 0 < n_pos < len(y) else np.nan
    rho = spearmanr(score, test_df["FINAL_POS"]).statistic  # want negative
    if is_prob is None:
        is_prob = score.min() >= 0 and score.max() <= 1
    return {"hits15": hits15, "auc": auc, "spearman_vs_pos": rho, "n_pos": n_pos,
            "field": len(test_df),
            "brier": float(np.mean((score - y) ** 2)) if is_prob else np.nan,
            "prob_sum": float(score.sum()) if is_prob else np.nan}
