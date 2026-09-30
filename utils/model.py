# utils/model.py
# The model: a percentile-regression forest on every past golfer-event,
# blended with the betting market. pga_api.model builds the rows (training and
# this week's field) and calls train_and_score with feature set 'stage7'.
#
# Forward-tested in experiments/sg_form_eval.py (2021-2026, trained only on
# earlier seasons): stage7's point-in-time strokes-gained ratings beat stage6's
# SG_FORM and prior-season stats on P_TOP20 Brier in 6 of 6 seasons.

import pandas as pd
from scipy.stats import rankdata
from sklearn.ensemble import RandomForestRegressor

from utils.features import normalize, feature_columns

RNG = 42


def train_and_score(training_df: pd.DataFrame, this_week: pd.DataFrame, variant: str = "stage7"):
    """Fit the percentile regressor on all training rows, score this week's
    field, and blend with the market. Returns (scored this_week, importances).

    SCORE (1 = best): within-field average of the model's rank and the market
    share's rank, rescaled to (0, 1]. MODEL_SCORE = 1 - predicted finish pct.
    variant: the feature set (utils.features.feature_columns); pga_api.model
    passes 'stage7'."""
    train_n, test_n = normalize(training_df.copy(), this_week.copy())
    fcols = feature_columns(train_n, include_field_size=True, variant=variant)

    missing = [c for c in fcols if c not in test_n.columns]
    if missing:
        raise ValueError(f"Current-week rows are missing feature columns: {missing}")
    assert not train_n[fcols].isna().any().any(), "NaNs remain in training features"
    assert not test_n[fcols].isna().any().any(), "NaNs remain in current-week features"

    reg = RandomForestRegressor(n_estimators=500, max_depth=8, min_samples_leaf=10,
                                random_state=RNG, n_jobs=-1)
    reg.fit(train_n[fcols], train_n["FINISH_PCT"])

    test_n["MODEL_SCORE"] = 1.0 - reg.predict(test_n[fcols])
    model_rank = rankdata(test_n["MODEL_SCORE"])
    market_rank = rankdata(test_n["ODDS_SHARE"])
    test_n["SCORE"] = ((model_rank + market_rank) / 2 / len(test_n)).round(4)
    # positive = model ranks the player higher than the market does
    test_n["LEVERAGE"] = (model_rank - market_rank).round(1)

    # P_TOP20: a true probability with magnitudes, for the optimizer objective.
    # SCORE is a rank blend (uniform steps — right for ordering, wrong for
    # summing under a salary cap: it flattens the elite premium). P_TOP20 is
    # the calibrated model P(top20) averaged with a market-implied P(top20)
    # learned by isotonic regression on relative market strength.
    # sum(P_TOP20 of a lineup) = expected number of top-20 finishers.
    from sklearn.calibration import CalibratedClassifierCV
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.isotonic import IsotonicRegression
    clf = CalibratedClassifierCV(
        RandomForestClassifier(n_estimators=500, max_depth=8, min_samples_leaf=10,
                               class_weight="balanced_subsample",
                               random_state=RNG, n_jobs=-1),
        method="isotonic", cv=3)
    clf.fit(train_n[fcols], train_n["TOP_20"])
    p_model = clf.predict_proba(test_n[fcols])[:, 1]
    iso = IsotonicRegression(increasing=True, out_of_bounds="clip")
    iso.fit(train_n["ODDS_SHARE"] * train_n["FIELD_SIZE"], train_n["TOP_20"])
    p_market = iso.predict(test_n["ODDS_SHARE"] * test_n["FIELD_SIZE"])
    test_n["P_TOP20"] = ((p_model + p_market) / 2).round(4)

    importances = (pd.Series(reg.feature_importances_, index=fcols)
                   .sort_values(ascending=False))
    return test_n.sort_values("P_TOP20", ascending=False).reset_index(drop=True), importances
