"""Two ideas the August 2026 feature tests left open, forward-tested on the
production model (stage7): course similarity, and a model/market blend that
varies by price tier.

    .venv/Scripts/python.exe experiments/course_blend_eval.py

Every test season is scored by models trained only on the seasons before it,
with the forward tests' metrics (experiments/metrics.py).

COURSE (feature arms, each added to production's 15 features)
  A course's demands come from its own ShotLink rounds, before the event: for
  each category, how widely it spreads the field there relative to the Tour as
  a whole (CR_OTT .. CR_PUTT; 1.2 = putting separates the field 20% more here).
    +profile   the four ratios as features (the forest can cross them with SGA_)
    +fit       SG_FIT = sum over categories of (ratio - 1) * the golfer's rating
    +similar   SG_SIM: strokes gained per round at the 8 courses whose profiles
               are nearest this one's (not this course), past 7 years, shrunk by
               2 phantom average rounds as SG_CH_SHRUNK is. Aimed at the golfers
               with no history at the course itself.

BLEND (on production's own forward predictions, no refit)
  P_TOP20 is today a flat 50/50 average of the model's calibrated P(top 20) and
  the market's. Tiers by odds rank within the field (1-5, 6-15, 16-30, 31-60,
  61+). For each test season the weights are chosen on the seasons before it
  only (from 2019), so nothing is tuned on the season it is scored on:
    tier_w     the model's weight per tier, grid 0..1, lowest Brier
    stack      logistic regression on both log-odds, per tier
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "experiments"))

from metrics import score_event  # noqa: E402
from pga_api import build, model as pmodel  # noqa: E402
from utils.features import feature_columns, normalize  # noqa: E402

TEST_SEASONS = [2021, 2022, 2023, 2024, 2025, 2026]
BLEND_FROM = 2019                 # base is also scored for 2019-2020, to choose blend weights
CATS = ["ott", "app", "arg", "putt"]
CR = [f"CR_{c.upper()}" for c in CATS]
NEW = CR + ["SG_FIT", "SG_SIM"]
MIN_COURSE_ROUNDS = 300           # ShotLink player-rounds before a course gets a profile
TOUR_YEARS = 3                    # the Tour-wide spread each ratio is measured against
K_SIMILAR, SIM_YEARS, SHRINK = 8, 7, 2
TIERS = [(1, 5), (6, 15), (16, 30), (31, 60), (61, 999)]
OUT = ROOT / "experiments" / "course_blend_eval_results.csv"


# ---------------------------------------------------------------- course features

def course_profiles(t: pd.DataFrame) -> pd.DataFrame:
    """Per event: each category's spread at its course / the Tour's, from ShotLink
    rounds of events that ended before it. -> TOURNAMENT, CR_*"""
    with sqlite3.connect(build.DB_PATH) as con:
        sg = pd.read_sql("SELECT * FROM sg_rounds", con)
    # Rounds the feed lists with every value exactly 0 are placeholders (the
    # non-ShotLink courses of a pro-am, and odd rounds elsewhere), not rounds.
    sg = sg[~(sg[[f"sg_{c}" for c in CATS] + ["sg_total"]] == 0).all(axis=1)]
    ev = t[["TOURNAMENT", "ENDING_DATE", "COURSE"]].drop_duplicates("TOURNAMENT")
    sg = sg.merge(ev, left_on="tournament_id", right_on="TOURNAMENT")
    # Sufficient statistics per past event: n, sum, sum of squares per category.
    agg = {"n": ("sg_ott", "size")}
    for c in CATS:
        sg[f"{c}2"] = sg[f"sg_{c}"] ** 2
        agg[f"s_{c}"] = (f"sg_{c}", "sum")
        agg[f"q_{c}"] = (f"{c}2", "sum")
    per = sg.groupby(["TOURNAMENT", "ENDING_DATE", "COURSE"]).agg(**agg).reset_index()

    def sd(block: pd.DataFrame, c: str) -> float:
        n = block["n"].sum()
        m = block[f"s_{c}"].sum() / n
        return float(np.sqrt(block[f"q_{c}"].sum() / n - m * m))

    rows = []
    for e in ev.itertuples():
        past = per[per["ENDING_DATE"] < e.ENDING_DATE]
        tour = past[past["ENDING_DATE"] >= e.ENDING_DATE - pd.DateOffset(years=TOUR_YEARS)]
        here = past[past["COURSE"] == e.COURSE]
        r = {"TOURNAMENT": e.TOURNAMENT}
        ok = here["n"].sum() >= MIN_COURSE_ROUNDS and tour["n"].sum() > 0
        for c, col in zip(CATS, CR):
            r[col] = sd(here, c) / sd(tour, c) if ok else np.nan
        rows.append(r)
    return pd.DataFrame(rows)


def similar_course_sg(t: pd.DataFrame, rounds: pd.DataFrame, prof: pd.DataFrame) -> pd.DataFrame:
    """SG_SIM per golfer-event: his shrunk strokes gained per round at the
    K_SIMILAR courses nearest this one's profile, as the profiles stood then."""
    ev = (t[["TOURNAMENT", "ENDING_DATE", "COURSE"]].drop_duplicates("TOURNAMENT")
          .merge(prof, on="TOURNAMENT").sort_values("ENDING_DATE"))
    out = []
    for e in ev.itertuples():
        if np.isnan(e.CR_OTT):
            continue
        # Each other course's latest profile before this event.
        past = ev[(ev["ENDING_DATE"] < e.ENDING_DATE) & (ev["COURSE"] != e.COURSE)].dropna(subset=CR)
        latest = past.groupby("COURSE")[CR].last()
        if len(latest) < K_SIMILAR:
            continue
        here = np.array([getattr(e, c) for c in CR])
        near = ((latest - here) ** 2).sum(axis=1).nsmallest(K_SIMILAR).index
        win = rounds[rounds["COURSE"].isin(near) &
                     (rounds["ENDING_DATE"] >= e.ENDING_DATE - pd.DateOffset(years=SIM_YEARS)) &
                     (rounds["ENDING_DATE"] < e.ENDING_DATE)]
        g = win.groupby("PLAYER")["SG"].agg(["sum", "count"])
        out.append(pd.DataFrame({"TOURNAMENT": e.TOURNAMENT, "PLAYER": g.index,
                                 "SG_SIM": (g["sum"] / (g["count"] + SHRINK)).values}))
    return pd.concat(out, ignore_index=True)


def event_rows() -> pd.DataFrame:
    rows, ctx = pmodel.training(as_of="2026-12-31")
    prof = course_profiles(ctx["t"])
    rows = rows.merge(prof, on="TOURNAMENT", how="left")
    rows = rows.merge(similar_course_sg(ctx["t"], ctx["rounds"], prof), on=["TOURNAMENT", "PLAYER"], how="left")
    fit = sum((rows[col] - 1) * rows[f"SGA_{c.upper()}"].fillna(0) for c, col in zip(CATS, CR))
    rows["SG_FIT"] = fit
    has_prof = rows["CR_OTT"].notna()
    print(f"course profile for {has_prof.mean():.0%} of golfer-events "
          f"({rows.loc[has_prof, 'TOURNAMENT'].nunique()} of {rows['TOURNAMENT'].nunique()} events); "
          f"SG_SIM for {rows['SG_SIM'].notna().mean():.0%}; of the golfer-events with no history at "
          f"the course, SG_SIM covers {rows.loc[rows['SG_CH_SHRUNK'].isna(), 'SG_SIM'].notna().mean():.0%}")
    print("course ratio spread (5th-95th pct): " + ", ".join(
        f"{c} {rows[c].quantile(.05):.2f}-{rows[c].quantile(.95):.2f}" for c in CR))
    # No profile -> an average course (ratio 1, no fit adjustment); no similar-course rounds -> 0.
    for c in CR:
        rows[c] = rows[c].fillna(1.0)
    rows["SG_FIT"] = rows["SG_FIT"].fillna(0.0)
    rows["SG_SIM"] = rows["SG_SIM"].fillna(0.0)
    return rows


# ---------------------------------------------------------------- forward scores

def season_scores(rows: pd.DataFrame) -> pd.DataFrame:
    from sklearn.calibration import CalibratedClassifierCV
    from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
    from sklearn.isotonic import IsotonicRegression

    out = []
    for season in range(BLEND_FROM, max(TEST_SEASONS) + 1):
        train, test = rows[rows["SEASON"] < season].copy(), rows[rows["SEASON"] == season].copy()
        if test.empty:
            continue
        train_n, test_n = normalize(train, test)
        base = [c for c in feature_columns(train_n, include_field_size=True, variant="stage7") if c not in NEW]
        arms = {"base": base}
        if season in TEST_SEASONS:
            arms.update({"+profile": base + CR, "+fit": base + ["SG_FIT"], "+similar": base + ["SG_SIM"]})
        iso = IsotonicRegression(increasing=True, out_of_bounds="clip").fit(
            train_n["ODDS_SHARE"] * train_n["FIELD_SIZE"], train_n["TOP_20"])
        p_market = iso.predict(test_n["ODDS_SHARE"] * test_n["FIELD_SIZE"])
        for name, f in arms.items():
            assert not train_n[f].isna().any().any() and not test_n[f].isna().any().any(), name
            reg = RandomForestRegressor(n_estimators=500, max_depth=8, min_samples_leaf=10,
                                        random_state=42, n_jobs=-1).fit(train_n[f], train_n["FINISH_PCT"])
            clf = CalibratedClassifierCV(RandomForestClassifier(
                n_estimators=500, max_depth=8, min_samples_leaf=10, class_weight="balanced_subsample",
                random_state=42, n_jobs=-1), method="isotonic", cv=3).fit(train_n[f], train_n["TOP_20"])
            sc = test_n[["TOURNAMENT", "SEASON", "PLAYER", "TOP_20", "FINAL_POS", "ODDS_SHARE"]].copy()
            sc["MODEL_SCORE"] = 1.0 - reg.predict(test_n[f])
            sc["P_MODEL"] = clf.predict_proba(test_n[f])[:, 1]
            sc["P_MARKET"] = p_market
            sc["P_TOP20"] = (sc["P_MODEL"] + sc["P_MARKET"]) / 2
            g = sc.groupby("TOURNAMENT")
            sc["SCORE"] = (g["MODEL_SCORE"].rank() + g["ODDS_SHARE"].rank()) / 2
            sc["MKT_RANK"] = g["ODDS_SHARE"].rank(ascending=False, method="average")
            sc["arm"] = name
            out.append(sc)
            if name != "base" and season == max(TEST_SEASONS):
                imp = pd.Series(reg.feature_importances_, index=f)
                print(f"    {name}: " + ", ".join(f"{c} {imp[c]:.3f} (#{int((imp > imp[c]).sum()) + 1}/{len(f)})"
                                               for c in f if c in NEW))
        print(f"  season {season}: {len(arms)} arm(s), trained on {len(train_n):,} rows", flush=True)
    return pd.concat(out, ignore_index=True)


# ---------------------------------------------------------------- blends

def tier_of(rank: pd.Series) -> pd.Series:
    out = pd.Series(len(TIERS) - 1, index=rank.index)
    for i, (lo, hi) in reversed(list(enumerate(TIERS))):
        out[(rank >= lo) & (rank < hi + 1)] = i
    return out


def _logit(p):
    p = np.clip(p, 1e-4, 1 - 1e-4)
    return np.log(p / (1 - p))


def blends(base: pd.DataFrame) -> pd.DataFrame:
    """Nested tier weights and a per-tier logistic stack, each season's choice
    made on the seasons before it. -> base's test-season rows with P_* columns."""
    from sklearn.linear_model import LogisticRegression

    b = base.copy()
    b["tier"] = tier_of(b["MKT_RANK"])
    grid = np.round(np.linspace(0, 1, 11), 1)

    def design(d):
        X = []
        for i in range(len(TIERS)):
            on = (d["tier"] == i).to_numpy().astype(float)
            X += [on, on * _logit(d["P_MODEL"].to_numpy()), on * _logit(d["P_MARKET"].to_numpy())]
        return np.column_stack(X)

    out, chosen = [], []
    for season in TEST_SEASONS:
        past, cur = b[b["SEASON"] < season], b[b["SEASON"] == season].copy()
        if cur.empty:
            continue
        w = {}
        for i in range(len(TIERS)):
            p = past[past["tier"] == i]
            briers = [np.mean((x * p["P_MODEL"] + (1 - x) * p["P_MARKET"] - p["TOP_20"]) ** 2) for x in grid]
            w[i] = grid[int(np.argmin(briers))]
        wt = cur["tier"].map(w)
        cur["P_TIER_W"] = wt * cur["P_MODEL"] + (1 - wt) * cur["P_MARKET"]
        lr = LogisticRegression(C=1e4, max_iter=2000, fit_intercept=False).fit(design(past), past["TOP_20"])
        cur["P_STACK"] = lr.predict_proba(design(cur))[:, 1]
        for x in (0.0, 0.25, 0.75, 1.0):
            cur[f"P_FLAT_{x:.2f}"] = x * cur["P_MODEL"] + (1 - x) * cur["P_MARKET"]
        chosen.append({"season": season, **{f"tier {lo}-{hi if hi < 999 else '+'}": w[i]
                                           for i, (lo, hi) in enumerate(TIERS)}})
        out.append(cur)
    print()
    print("model weight chosen per tier (from earlier seasons; production is 0.5 everywhere):")
    print(pd.DataFrame(chosen).set_index("season").to_string())
    return pd.concat(out, ignore_index=True)


def tier_diagnostic(b: pd.DataFrame) -> None:
    """Within each tier: who separates top-20 finishers better, the model or the
    market (pooled AUC), and each one's calibration (mean predicted v actual)."""
    from sklearn.metrics import roc_auc_score
    b = b.assign(tier=tier_of(b["MKT_RANK"]))
    rows = []
    for i, (lo, hi) in enumerate(TIERS):
        d = b[b["tier"] == i]
        rows.append({"tier": f"{lo}-{hi if hi < 999 else '+'}", "rows": len(d), "top20_rate": d["TOP_20"].mean(),
                     "p_model": d["P_MODEL"].mean(), "p_market": d["P_MARKET"].mean(),
                     "auc_model": roc_auc_score(d["TOP_20"], d["P_MODEL"]),
                     "auc_market": roc_auc_score(d["TOP_20"], d["P_MARKET"])})
    print()
    print("within each price tier, test seasons pooled:")
    print(pd.DataFrame(rows).round(3).to_string(index=False))


# ---------------------------------------------------------------- report

def evaluate(sc: pd.DataFrame, cols: dict) -> pd.DataFrame:
    """cols: label -> (arm rows to use, probability or score column)."""
    res = []
    for label, (arm, col) in cols.items():
        d = sc[sc["arm"] == arm]
        for tid, g in d.groupby("TOURNAMENT"):
            m = score_event(g, g[col].to_numpy(), is_prob=(col != "SCORE"))
            res.append({"variant": label, "tournament_id": tid, "season": int(g["SEASON"].iloc[0]), **m})
    return pd.DataFrame(res)


def report(res: pd.DataFrame, control: str, title: str) -> None:
    base = res[res["variant"] == control].set_index("tournament_id")
    rows = []
    for v, d in res.groupby("variant", sort=False):
        d = d.set_index("tournament_id")
        dh, db, da = (d["hits15"] - base["hits15"]), (d["brier"] - base["brier"]), (d["auc"] - base["auc"])
        se = lambda x: x.std(ddof=1) / np.sqrt(len(x)) if v != control else np.nan
        by_h = d.groupby("season")["hits15"].mean() - base.groupby("season")["hits15"].mean()
        by_b = d.groupby("season")["brier"].mean() - base.groupby("season")["brier"].mean()
        rows.append({"variant": v, "events": len(d), "hits15": d["hits15"].mean(), "auc": d["auc"].mean(),
                     "brier_e4": d["brier"].mean() * 1e4,
                     "d_hits15": dh.mean(), "t_hits": dh.mean() / se(dh),
                     "d_auc_e4": da.mean() * 1e4, "d_brier_e4": db.mean() * 1e4, "t_brier": db.mean() / se(db),
                     "seasons_better": f"{int((by_h > 0).sum())}/{len(by_h)} hits, {int((by_b < 0).sum())}/{len(by_b)} Brier"})
    print()
    print(f"=== {title} (against {control}; Brier lower is better) ===")
    print(pd.DataFrame(rows).round(3).to_string(index=False))


def main():
    rows = event_rows()
    sc = season_scores(rows)
    test = sc[sc["SEASON"].isin(TEST_SEASONS)]
    feat = evaluate(test, {"production": ("base", "P_TOP20"), "+profile": ("+profile", "P_TOP20"),
                           "+fit": ("+fit", "P_TOP20"), "+similar": ("+similar", "P_TOP20")})
    report(feat, "production", "course features, P_TOP20")
    # Where +similar should help: golfers with no history at the course.
    b = sc[sc["arm"] == "base"]
    bl = blends(b)
    bl["arm"] = "base"
    tier_diagnostic(bl)
    blend = evaluate(bl, {"production (50/50)": ("base", "P_TOP20"), "market only": ("base", "P_FLAT_0.00"),
                          "model 25%": ("base", "P_FLAT_0.25"), "model 75%": ("base", "P_FLAT_0.75"),
                          "model only": ("base", "P_FLAT_1.00"), "tier weights": ("base", "P_TIER_W"),
                          "tier stack": ("base", "P_STACK")})
    report(blend, "production (50/50)", "blends of production's own predictions")
    pd.concat([feat.assign(test="course"), blend.assign(test="blend")]).to_csv(OUT, index=False)
    print()
    print(f"per-event results -> {OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
