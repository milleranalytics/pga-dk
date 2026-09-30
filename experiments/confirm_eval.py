"""Confirmation run for the three changes experiments/course_blend_eval.py
turned up, alone and together, on a fresh random seed:

    .venv/Scripts/python.exe experiments/confirm_eval.py

    blend      P_TOP20 = 75% model + 25% market (production: 50/50). The 75% was
               picked after seeing 2021-2026, so each season is also scored with
               the flat weight chosen on the seasons before it only ("nested").
    profile    the course's four dispersion ratios CR_OTT..CR_PUTT as features.
    sgfix      sg_form recomputed without sg_rounds' placeholder rounds (every
               value exactly 0; 2015-2023), which pull category ratings toward 0.

Arms (each fitted on seasons before the test season, as production would be):
base, +profile, sgfix, sgfix+profile; each scored at 50/50, 75/25 and nested.
Seed SEED here; course_blend_eval.py used 42, and its per-event results are
folded in where the same comparison exists, as a second seed.
"""

from __future__ import annotations

import sqlite3
import sys
import tempfile
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "experiments"))

import course_blend_eval as cbe  # noqa: E402
from pga_api import build, sg  # noqa: E402
from utils.features import feature_columns, normalize  # noqa: E402

SEED = 7
TEST_SEASONS = cbe.TEST_SEASONS
FIRST = cbe.BLEND_FROM
GRID = np.round(np.linspace(0, 1, 21), 2)
CACHE = Path(tempfile.gettempdir()) / "pga_sg_form_nozero.pkl"
PREV = ROOT / "experiments" / "course_blend_eval_results.csv"
OUT = ROOT / "experiments" / "confirm_eval_results.csv"


def sg_form_fixed() -> pd.DataFrame:
    """sg_form as build.py makes it, from sg_rounds without the placeholders."""
    if CACHE.exists():
        return pd.read_pickle(CACHE)
    with sqlite3.connect(build.DB_PATH) as con:
        frames = {k: pd.read_sql(f"SELECT * FROM {k}", con)
                  for k in ("events", "rounds", "kft_events", "kft_rounds", "sg_rounds", "results", "field")}
    s = frames["sg_rounds"]
    zero = (s[["sg_ott", "sg_app", "sg_arg", "sg_putt", "sg_total"]] == 0).all(axis=1)
    print(f"sgfix: dropping {zero.sum():,} placeholder rounds of {len(s):,}; refitting sg_form ...", flush=True)
    frames["sg_rounds"] = s[~zero]
    t0 = time.time()
    form = sg.form_table(frames)
    print(f"sgfix: sg_form refitted in {time.time() - t0:.0f}s", flush=True)
    form.to_pickle(CACHE)
    return form


def with_form(rows: pd.DataFrame, form: pd.DataFrame) -> pd.DataFrame:
    f = form.drop(columns="as_of").rename(columns={"tournament_id": "TOURNAMENT", "player_id": "PLAYER"})
    out = rows.drop(columns=sg.FORM_COLS).merge(f, on=["TOURNAMENT", "PLAYER"], how="left")
    assert len(out) == len(rows)
    return out


def fit_arm(train, test, feats):
    from sklearn.calibration import CalibratedClassifierCV
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.isotonic import IsotonicRegression
    assert not train[feats].isna().any().any() and not test[feats].isna().any().any()
    clf = CalibratedClassifierCV(RandomForestClassifier(
        n_estimators=500, max_depth=8, min_samples_leaf=10, class_weight="balanced_subsample",
        random_state=SEED, n_jobs=-1), method="isotonic", cv=3).fit(train[feats], train["TOP_20"])
    iso = IsotonicRegression(increasing=True, out_of_bounds="clip").fit(
        train["ODDS_SHARE"] * train["FIELD_SIZE"], train["TOP_20"])
    sc = test[["TOURNAMENT", "SEASON", "PLAYER", "TOP_20", "FINAL_POS", "ODDS_SHARE"]].copy()
    sc["P_MODEL"] = clf.predict_proba(test[feats])[:, 1]
    sc["P_MARKET"] = iso.predict(test["ODDS_SHARE"] * test["FIELD_SIZE"])
    return sc


def season_scores(data: dict) -> pd.DataFrame:
    """data: arm family -> rows. Families 'base' and 'sgfix'; each also gets a
    '+profile' arm in the test seasons."""
    out = []
    for season in range(FIRST, max(TEST_SEASONS) + 1):
        t0 = time.time()
        for fam, rows in data.items():
            train, test = rows[rows["SEASON"] < season].copy(), rows[rows["SEASON"] == season].copy()
            train_n, test_n = normalize(train, test)
            base = [c for c in feature_columns(train_n, include_field_size=True, variant="stage7")
                    if c not in cbe.NEW]
            arms = {fam: base}
            if season in TEST_SEASONS:
                arms[f"{fam}+profile"] = base + cbe.CR
            for arm, f in arms.items():
                out.append(fit_arm(train_n, test_n, f).assign(arm=arm))
        print(f"  season {season} done in {time.time() - t0:.0f}s", flush=True)
    return pd.concat(out, ignore_index=True)


def add_blends(sc: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """P at 50/50, 75/25, and the flat weight chosen on earlier seasons (of the
    arm's family, whose 2019-2020 scores exist)."""
    sc = sc.copy()
    for x in (0.5, 0.75):
        sc[f"P_{int(x * 100)}"] = x * sc["P_MODEL"] + (1 - x) * sc["P_MARKET"]
    sc["P_NESTED"] = np.nan
    chosen = []
    for fam in ("base", "sgfix"):
        hist = sc[sc["arm"] == fam]
        for season in TEST_SEASONS:
            p = hist[hist["SEASON"] < season]
            briers = [np.mean((x * p["P_MODEL"] + (1 - x) * p["P_MARKET"] - p["TOP_20"]) ** 2) for x in GRID]
            w = GRID[int(np.argmin(briers))]
            chosen.append({"family": fam, "season": season, "model_weight": w})
            m = sc["arm"].str.startswith(fam) & (sc["SEASON"] == season)
            sc.loc[m, "P_NESTED"] = w * sc.loc[m, "P_MODEL"] + (1 - w) * sc.loc[m, "P_MARKET"]
    # Keep 50/50's ordering, fix its calibration: a monotone map from P_50 to
    # the rate, fitted on earlier seasons (Platt on log-odds; isotonic).
    from sklearn.isotonic import IsotonicRegression
    from sklearn.linear_model import LogisticRegression
    sc["P_50_PLATT"] = np.nan
    sc["P_50_ISO"] = np.nan
    for arm in sc["arm"].unique():
        fam = "sgfix" if arm.startswith("sgfix") else "base"
        hist = sc[sc["arm"] == fam]
        for season in TEST_SEASONS:
            p = hist[hist["SEASON"] < season]
            m = (sc["arm"] == arm) & (sc["SEASON"] == season)
            lr = LogisticRegression(C=1e6).fit(cbe._logit(p["P_50"].to_numpy())[:, None], p["TOP_20"])
            sc.loc[m, "P_50_PLATT"] = lr.predict_proba(cbe._logit(sc.loc[m, "P_50"].to_numpy())[:, None])[:, 1]
            iso = IsotonicRegression(out_of_bounds="clip").fit(p["P_50"], p["TOP_20"])
            sc.loc[m, "P_50_ISO"] = iso.predict(sc.loc[m, "P_50"])
    return sc, pd.DataFrame(chosen).pivot(index="season", columns="family", values="model_weight")


def main():
    rows = cbe.event_rows()
    fixed = with_form(rows, sg_form_fixed())
    moved = (fixed[sg.FORM_COLS] - rows[sg.FORM_COLS]).abs()
    print("sgfix: mean |change| per rating, by season: ")
    print(moved.groupby(rows["SEASON"]).mean().round(3).loc[FIRST:].to_string())

    scores = Path(tempfile.gettempdir()) / f"pga_confirm_scores_{SEED}.pkl"
    if scores.exists():
        sc = pd.read_pickle(scores)
    else:
        sc = season_scores({"base": rows, "sgfix": fixed})
        sc.to_pickle(scores)
    sc, chosen = add_blends(sc)
    print()
    print("flat model weight chosen on earlier seasons (production 0.5):")
    print(chosen.to_string())

    test = sc[sc["SEASON"].isin(TEST_SEASONS)]
    variants = {
        "production (base 50/50)": ("base", "P_50"),
        "blend 75/25": ("base", "P_75"),
        "blend nested": ("base", "P_NESTED"),
        "50/50 recalibrated (Platt)": ("base", "P_50_PLATT"),
        "50/50 recalibrated (isotonic)": ("base", "P_50_ISO"),
        "+profile 50/50": ("base+profile", "P_50"),
        "+profile 50/50 Platt": ("base+profile", "P_50_PLATT"),
        "+profile 75/25": ("base+profile", "P_75"),
        "sgfix 50/50": ("sgfix", "P_50"),
        "sgfix 75/25": ("sgfix", "P_75"),
        "sgfix 50/50 Platt": ("sgfix", "P_50_PLATT"),
        "sgfix+profile 75/25": ("sgfix+profile", "P_75"),
        "sgfix+profile nested": ("sgfix+profile", "P_NESTED"),
    }
    res = cbe.evaluate(test, variants)
    cbe.report(res, "production (base 50/50)", f"confirmation, seed {SEED}")
    # The profile and blend on their own terms: each against the arm it modifies.
    cbe.report(res[res["variant"].isin(["blend 75/25", "+profile 75/25", "sgfix 75/25", "sgfix+profile 75/25"])],
               "blend 75/25", "at 75/25: what profile and sgfix add")

    if PREV.exists():
        prev = pd.read_csv(PREV)
        pairs = {"+profile vs base, 50/50": (("course", "+profile"), ("course", "production"),
                                             "+profile 50/50", "production (base 50/50)"),
                 "75/25 vs 50/50": (("blend", "model 75%"), ("blend", "production (50/50)"),
                                    "blend 75/25", "production (base 50/50)")}
        print()
        print(f"=== both seeds (42 from course_blend_eval, {SEED} here): mean per-event difference ===")
        for label, (a, b, na, nb) in pairs.items():
            d42 = (prev[(prev["test"] == a[0]) & (prev["variant"] == a[1])].set_index("tournament_id")[["hits15", "brier"]]
                   - prev[(prev["test"] == b[0]) & (prev["variant"] == b[1])].set_index("tournament_id")[["hits15", "brier"]])
            dn = (res[res["variant"] == na].set_index("tournament_id")[["hits15", "brier"]]
                  - res[res["variant"] == nb].set_index("tournament_id")[["hits15", "brier"]])
            for seed, d in (("42", d42), (str(SEED), dn), ("avg", (d42 + dn) / 2)):
                t = lambda x: x.mean() / (x.std(ddof=1) / np.sqrt(len(x)))
                print(f"  {label:26s} seed {seed:>3s}: hits {d['hits15'].mean():+.3f} (t {t(d['hits15']):+.2f}), "
                      f"Brier {d['brier'].mean() * 1e4:+.2f}e-4 (t {t(d['brier']):+.2f})")
    res.to_csv(OUT, index=False)
    print()
    print(f"per-event results -> {OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
