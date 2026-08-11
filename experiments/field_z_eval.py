# experiments/field_z_eval.py
# stage8_eval showed SG_FORM_Z arriving at importance #3 of 43 and the model
# arm up 0.077 hits, but it tested Z ADDED ALONGSIDE SG_FORM. Those two are
# near-collinear, so the forest splits its attention between them and neither
# the gain nor the importance is clean.
#
# The actual question — should form be measured against the field instead of on
# an absolute scale — is a SWAP. Arms:
#
#   s6         control, absolute SG_FORM
#   z_swap     SG_FORM replaced by SG_FORM_Z
#   z_ctx      the swap, plus the field's mean and SD as context columns
#   z_add      Z alongside SG_FORM (stage8's +field, repeated here for reference)
#
# Same forward chain, same metrics, same control as every other experiment.

import time
import sys
import os
import numpy as np
import pandas as pd
from scipy.stats import rankdata
from sklearn.ensemble import RandomForestRegressor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.features import (load_tables, list_events, build_event_rows, build_rounds,
                            normalize, feature_columns)
from experiments.forward_eval import score_event, event_key, DB, ALL_SEASONS, TEST_SEASONS, RNG

# (columns to drop, columns to add)
CANDIDATES = {
    "s6":     ([], []),
    "z_swap": (["SG_FORM"], ["SG_FORM_Z"]),
    "z_ctx":  (["SG_FORM"], ["SG_FORM_Z", "FIELD_SG_MEAN", "FIELD_SG_SD"]),
    "z_add":  ([], ["SG_FORM_Z", "FIELD_SG_MEAN", "FIELD_SG_SD"]),
}


def main():
    t0 = time.time()
    t, s, o = load_tables(DB)
    rounds = build_rounds(t)
    events = list_events(t, ALL_SEASONS)
    cache = {}
    for _, ev in events.iterrows():
        cache[event_key(ev)] = build_event_rows(t, s, o, ev, exclude_wd=True, rounds=rounds,
                                                candidates=True)
    events["KEY"] = [event_key(ev) for _, ev in events.iterrows()]
    print(f"{len(events)} events cached in {time.time()-t0:.0f}s")

    results = []
    for season in TEST_SEASONS:
        train = pd.concat([cache[k] for k in events[events["SEASON"] < season]["KEY"]],
                          ignore_index=True)
        season_tests = events[events["SEASON"] == season]
        tests = {k: cache[k].copy() for k in season_tests["KEY"] if not cache[k].empty}
        merged = pd.concat(tests.values(), ignore_index=True)
        train_n, merged = normalize(train.copy(), merged)
        merged["KEY2"] = np.repeat([str(k) for k in tests.keys()],
                                   [len(v) for v in tests.values()])

        base = feature_columns(train_n, include_field_size=True, variant="stage6")
        for arm, (drop, add) in CANDIDATES.items():
            fcols = [c for c in base if c not in drop] + add
            reg = RandomForestRegressor(n_estimators=500, max_depth=8, min_samples_leaf=10,
                                        random_state=RNG, n_jobs=-1)
            reg.fit(train_n[fcols], train_n["FINISH_PCT"])
            merged[f"M_{arm}"] = 1.0 - reg.predict(merged[fcols])
            if season == TEST_SEASONS[-1]:
                imp = pd.Series(reg.feature_importances_, index=fcols).sort_values(ascending=False)
                ranks = list(imp.index)
                shown = [c for c in ["SG_FORM", "SG_FORM_Z", "ODDS_SHARE"] if c in fcols]
                print(f"  [{season}] {arm:7s} " + ", ".join(
                    f"{c} {imp[c]:.4f} (#{ranks.index(c)+1}/{len(fcols)})" for c in shown))

        for _, ev in season_tests.iterrows():
            sub = merged[merged["KEY2"] == str(ev["KEY"])]
            if sub.empty or sub["TOP_20"].nunique() < 1:
                continue
            mkt = rankdata(sub["ODDS_SHARE"])
            for arm in CANDIDATES:
                for mode, sc in [("model", sub[f"M_{arm}"].to_numpy()),
                                 ("blend", (rankdata(sub[f"M_{arm}"]) + mkt) / 2)]:
                    m = score_event(sub, sc, is_prob=False)
                    m.update(arm=arm, mode=mode, SEASON=season,
                             TOURNAMENT=ev["TOURNAMENT"],
                             ENDING_DATE=str(pd.Timestamp(ev["ENDING_DATE"]).date()))
                    results.append(m)
        print(f"season {season} done {time.time()-t0:.0f}s")

    res = pd.DataFrame(results)
    res.to_csv(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "field_z_eval_results.csv"), index=False)

    order = list(CANDIDATES)
    for mode in ("blend", "model"):
        sub = res[res["mode"] == mode]
        summ = sub.groupby("arm").agg(events=("hits15", "count"), hits15=("hits15", "mean"),
                                      auc=("auc", "mean"),
                                      spearman=("spearman_vs_pos", "mean")).loc[order]
        summ["d_hits15"] = (summ["hits15"] - summ.loc["s6", "hits15"]).round(3)
        summ["d_auc"] = (summ["auc"] - summ.loc["s6", "auc"]).round(4)
        wins, tstat = [], []
        b = sub[sub.arm == "s6"].set_index(["SEASON", "TOURNAMENT", "ENDING_DATE"])["hits15"]
        for arm in order:
            a = sub[sub.arm == arm].set_index(["SEASON", "TOURNAMENT", "ENDING_DATE"])["hits15"]
            d = (a - b).dropna()
            wins.append(f"{int((d > 0).sum())}-{int((d < 0).sum())}-{int((d == 0).sum())}")
            tstat.append(round(float(d.mean() / (d.std(ddof=1) / np.sqrt(len(d)))), 2)
                         if arm != "s6" and d.std(ddof=1) > 0 else np.nan)
        summ["w-l-t"] = wins
        summ["t"] = tstat
        print(f"\n=== {mode.upper()} · {len(sub)//len(order)} events ===")
        print(summ.round(3).to_string())

    print("\n=== hits@15 by season (model) ===")
    print(res[res["mode"] == "model"].pivot_table(
        index="SEASON", columns="arm", values="hits15", aggfunc="mean")[order].round(3).to_string())
    print(f"\nTotal time: {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
