"""Train, score, log and grade, on pga.db.

The feature functions and the model are utils/features.py and utils/model.py:
pga_api.legacy hands them pga.db in the name-keyed shape they were written
for (golf.db's), keyed by the Tour's ids instead. pga_api.validate holds the
checks that nothing reads the future.
"""

from __future__ import annotations

import io
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from pga_api import build, legacy, sg
from pga_api.names import normalize_name
from utils.features import (add_market_share, build_event_rows, build_rounds, list_events,
                            rolling_features_for_event, sg_at_course_for_event,
                            sg_features_for_event)
from utils import model as _model

# The feature set: stage6 plus sg_form's strokes-gained ratings in place of
# SG_FORM and last season's stats (utils.features.feature_columns).
VARIANT = "stage7"

DATA = Path(__file__).resolve().parent.parent / "data"
PRED_DIR = DATA / "predictions"
EXPORT_CSV = DATA / "api_week_export.csv"
FIRST_SEASON = 2016

EXPORT_COLS = ["PLAYER", "SALARY", "P_TOP20", "SCORE", "MODEL_SCORE", "ODDS_SHARE", "LEVERAGE",
               "VEGAS_ODDS", "SG_FORM", "PCT_FORM_SHRUNK", "SG_CH_SHRUNK",
               "CUT_PERCENTAGE", "FEDEX_CUP_POINTS", "OWGR_RANK"]
ROUNDING = {"P_TOP20": 3, "MODEL_SCORE": 4, "ODDS_SHARE": 4, "LEVERAGE": 1, "SG_FORM": 2,
            "PCT_FORM_SHRUNK": 3, "SG_CH_SHRUNK": 2, "CUT_PERCENTAGE": 1,
            "FEDEX_CUP_POINTS": 0, "OWGR_RANK": 0}


def display_names() -> pd.Series:
    """player_id -> the name shown everywhere: the Tour's, accents dropped as golf.db did.

    The dashboard joins on names, so two golfers may not share one: the one with
    fewer starts is shown by his spelling in data/player_aliases.csv if it has
    one no one else uses ('Zach J. Johnson', as DraftKings and the Tour's entry
    list have it), otherwise with his id, 'Zach Johnson (29747)'."""
    from pga_api.identity import ALIASES
    with sqlite3.connect(build.DB_PATH) as con:
        pl = pd.read_sql("SELECT player_id, name FROM players", con)
        starts = pd.read_sql("SELECT player_id, COUNT(*) n FROM results GROUP BY player_id", con)
    pl["shown"] = pl["name"].map(normalize_name)
    pl = pl.merge(starts, on="player_id", how="left").fillna({"n": 0})
    pl = pl.sort_values("n", ascending=False)
    dup = pl.duplicated("shown", keep="first")
    spelled = (pd.read_csv(ALIASES, dtype=str).assign(name=lambda a: a["name"].map(normalize_name))
               .drop_duplicates("player_id").set_index("player_id")["name"]
               if ALIASES.exists() else pd.Series(dtype=str))
    alt = pl["player_id"].map(spelled)
    alt = alt.where(dup & alt.notna() & ~alt.isin(pl["shown"]) & ~alt.duplicated(keep=False))
    pl["shown"] = alt.fillna(pl["shown"].where(~dup, pl["shown"] + " (" + pl["player_id"] + ")"))
    return pl.set_index("player_id")["shown"]


# ---------------------------------------------------------------- train

def training(as_of, first_season: int = FIRST_SEASON, verbose: bool = True):
    """Every stroke-play event from first_season up to (not including) `as_of`,
    each with its features as they stood before it. -> (rows, context)"""
    as_of = pd.Timestamp(as_of)
    t, s, o = legacy.tables()
    rounds = build_rounds(t)
    events = list_events(t, list(range(first_season, as_of.year + 1)))
    events = events[events["ENDING_DATE"] < as_of]
    rows = pd.concat([build_event_rows(t, s, o, ev, exclude_wd=True, rounds=rounds)
                      for _, ev in events.iterrows()], ignore_index=True)
    with sqlite3.connect(build.DB_PATH) as con:
        form = pd.read_sql("SELECT * FROM sg_form", con)
    rows = rows.merge(form.drop(columns="as_of").rename(columns={"tournament_id": "TOURNAMENT",
                                                                 "player_id": "PLAYER"}),
                      on=["TOURNAMENT", "PLAYER"], how="left")
    if verbose:
        print(f"training: {len(rows):,} golfer-events from {len(events)} events "
              f"({events['SEASON'].min()}-{events['SEASON'].max()}), "
              f"odds for {rows['VEGAS_ODDS'].notna().mean():.0%}, "
              f"strokes-gained ratings for {rows['SGA_TOTAL'].notna().mean():.0%}")
    return rows, {"t": t, "s": s, "o": o, "rounds": rounds}


def sg_ratings(as_of) -> pd.DataFrame:
    """Every golfer's strokes-gained ratings as of `as_of` (an event's first
    day), fitted on pga.db's rounds of events that ended before it."""
    with sqlite3.connect(build.DB_PATH) as con:
        tb = {k: pd.read_sql(f"SELECT * FROM {k}", con)
              for k in ("events", "rounds", "kft_events", "kft_rounds", "sg_rounds")}
    rows = sg.round_rows(tb["events"], tb["rounds"], tb["kft_events"], tb["kft_rounds"], tb["sg_rounds"])
    return sg.ratings(rows, as_of)


def train_and_score(training_df: pd.DataFrame, this_week: pd.DataFrame):
    """utils.model.train_and_score on this pipeline's feature set (VARIANT)."""
    return _model.train_and_score(training_df, this_week, variant=VARIANT)


# ---------------------------------------------------------------- importances

# Feature -> (group, label). The groups are what the chart colours by: the seven
# ratings share credit between them (SGA_T2G is three of the others added up),
# so the group's total says more than any one bar.
FEATURE_LABELS = {
    "ODDS_SHARE": ("Market", "Odds share"),
    "SGA_TOTAL": ("Strokes-gained ratings", "SG total"),
    "SGA_T2G": ("Strokes-gained ratings", "SG tee to green"),
    "SGA_OTT": ("Strokes-gained ratings", "SG off the tee"),
    "SGA_APP": ("Strokes-gained ratings", "SG approach"),
    "SGA_ARG": ("Strokes-gained ratings", "SG around the green"),
    "SGA_PUTT": ("Strokes-gained ratings", "SG putting"),
    "SGA_ROUNDS_12M": ("Strokes-gained ratings", "Rounds, last 12 months"),
    "PCT_FORM_SHRUNK": ("Results & course", "Finish percentile, 9 mo"),
    "CUT_PERCENTAGE": ("Results & course", "Cuts made %, 9 mo"),
    "CONSECUTIVE_CUTS": ("Results & course", "Consecutive cuts"),
    "FEDEX_CUP_POINTS": ("Results & course", "FedEx points, 9 mo"),
    "form_density": ("Results & course", "FedEx points per start"),
    "SG_CH_SHRUNK": ("Results & course", "SG at this course"),
    "FIELD_SIZE": ("Field size", "Field size"),
}
# Categorical slots 1-3 of the dataviz reference palette (dark steps, for
# plotly_dark), then grey: field size is the same for every golfer in a week.
GROUP_COLORS = {"Strokes-gained ratings": "#3987e5", "Market": "#d95926",
                "Results & course": "#199e70", "Field size": "#898781"}


def importance_chart(importances: pd.Series):
    """How much each feature drives the finish-percentile forest
    (train_and_score's importances), coloured by kind, with each kind's total
    in the legend.

    FIELD_SIZE ranks high but is the same for every golfer in a week: it helps
    the forest scale finish percentiles across fields of different sizes and
    cannot reorder this week's field."""
    import plotly.graph_objects as go

    imp = importances.sort_values()
    groups = {f: FEATURE_LABELS.get(f, ("Results & course", f))[0] for f in imp.index}
    totals = pd.Series(imp.values, index=[groups[f] for f in imp.index]).groupby(level=0).sum()
    fig = go.Figure()
    for g in GROUP_COLORS:
        feats = [f for f in imp.index if groups[f] == g]
        if not feats:
            continue
        fig.add_bar(
            y=[FEATURE_LABELS.get(f, (g, f))[1] for f in feats], x=imp[feats], orientation="h",
            name=f"{g}  {totals[g]:.0%}", marker={"color": GROUP_COLORS[g], "cornerradius": 4},
            text=[f"{v:.1%}" for v in imp[feats]], textposition="outside",
            textfont={"color": "#c3c2b7", "size": 11}, cliponaxis=False,
            customdata=feats, hovertemplate="%{customdata}: %{x:.1%}<extra></extra>")
    order = [FEATURE_LABELS.get(f, (None, f))[1] for f in imp.index]
    fig.update_layout(
        title={"text": "What the model leans on<br><sup>share of the finish-percentile "
                       "forest's splits; field size is identical within a week</sup>"},
        template="plotly_dark", barmode="overlay", bargap=0.3, height=40 + 26 * len(imp) + 90,
        margin={"l": 10, "r": 50, "t": 70, "b": 30},
        xaxis={"tickformat": ".0%", "gridcolor": "#2c2c2a", "zeroline": False},
        yaxis={"categoryorder": "array", "categoryarray": order, "ticksuffix": "  "},
        legend={"orientation": "h", "y": -0.08, "x": 0, "traceorder": "normal"})
    return fig


# ---------------------------------------------------------------- this week

def week_rows(ctx: dict, week, dk: pd.DataFrame, odds: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    """Features for this week's priced field, exactly as training builds them.

    `dk` is weekly.prices() (player_id, salary); `odds` has player_id and
    VEGAS_ODDS (fractional odds as a number: 12/1 -> 12.0). A priced golfer the
    Tour cannot identify keeps a row, with fills where his history would be."""
    t, s, rounds = ctx["t"], ctx["s"], ctx["rounds"]
    end = pd.Timestamp(week.end_date)
    course = legacy.week_course_key(week.tournament_id)

    df = pd.DataFrame({"PLAYER": dk["player_id"].fillna("dk:" + dk["dk_name"]).values,
                       "SALARY": dk["salary"].values})
    stats = s[s["SEASON"] == week.season - 1].drop_duplicates("PLAYER").drop(columns="SEASON")
    df = df.merge(stats, on="PLAYER", how="left")
    o = odds.dropna(subset=["player_id"]).drop_duplicates("player_id")
    df = df.merge(o[["player_id", "VEGAS_ODDS"]].rename(columns={"player_id": "PLAYER"}),
                  on="PLAYER", how="left")
    roll = rolling_features_for_event(t, end, course, exclude_wd=True)
    df = df.merge(roll["window"][["PLAYER", "CUT_PERCENTAGE", "FEDEX_CUP_POINTS", "form_density",
                                  "CONSECUTIVE_CUTS", "RECENT_FORM", "adj_form", "PCT_FORM_SHRUNK"]],
                  on="PLAYER", how="left")
    df = df.merge(roll["course"], on="PLAYER", how="left")
    df = df.merge(sg_features_for_event(rounds, end), on="PLAYER", how="left")
    df = df.merge(sg_at_course_for_event(rounds, end, course), on="PLAYER", how="left")
    df = df.merge(sg_ratings(week.start_date), left_on="PLAYER", right_index=True, how="left")
    # This week's world ranking: this season's OWGR stat, re-fetched on every
    # build. For display; training rows carry no such column, so the model
    # cannot pick it up.
    owgr_now = (s[s["SEASON"] == week.season][["PLAYER", "OWGR_RANK", "OWGR"]].drop_duplicates("PLAYER")
                .rename(columns={"OWGR_RANK": "OWGR_NOW_RANK", "OWGR": "OWGR_NOW"}))
    df = df.merge(owgr_now, on="PLAYER", how="left")
    df = add_market_share(df)
    df["FIELD_SIZE"] = len(df)
    if verbose:
        for label, col in (("odds", "VEGAS_ODDS"), ("strokes-gained ratings", "SGA_TOTAL"),
                           ("strokes-gained categories", "SGA_APP")):
            print(f"  {label}: {df[col].notna().mean():.0%} of the priced field")
        print(f"  course history at this course: {df['SG_CH_SHRUNK'].notna().sum()} golfers")
        _report_gaps(df, week.tournament_id)
    return df


def _report_gaps(df: pd.DataFrame, tournament_id: str, min_salary: int = 7000) -> None:
    """The priced golfers to look at before building lineups, by name:

    withdrawn   priced by DraftKings but OUT on the Tour's entry list (any salary)
    not entered priced by DraftKings but absent from a published entry list
                (any salary): not playing, or matched to the wrong golfer
    no odds     priced $7,000+ with no price on the board: a board that has not
                priced him yet, or a withdrawal the entry list has not caught
    no rating   priced $7,000+ with no strokes-gained rating (filled as below
                average); normal for a rookie, not for a regular"""
    names = display_names()
    with sqlite3.connect(build.DB_PATH) as con:
        f = pd.read_sql("SELECT player_id, withdrawn FROM field WHERE tournament_id = ?",
                        con, params=(tournament_id,))
    out = set(f.loc[f["withdrawn"] == 1, "player_id"])
    g = df.assign(withdrawn=df["PLAYER"].isin(out),
                  not_entered=~df["PLAYER"].isin(set(f["player_id"])) & (len(f) > 0),
                  no_odds=df["VEGAS_ODDS"].isna(), no_rating=df["SGA_TOTAL"].isna())
    g = g[g["withdrawn"] | g["not_entered"]
          | ((g["no_odds"] | g["no_rating"]) & (g["SALARY"] >= min_salary))]
    if g.empty:
        print(f"  no withdrawals; every golfer priced ${min_salary:,}+ has odds and a rating.")
        return
    print("  look at before building lineups:")
    for r in g.sort_values("SALARY", ascending=False).itertuples():
        what = ", ".join(w for w, on in (("WITHDRAWN (entry list)", r.withdrawn),
                                         ("NOT ON the entry list", r.not_entered),
                                         ("no odds", r.no_odds), ("no rating", r.no_rating)) if on)
        name = names.get(r.PLAYER, str(r.PLAYER).removeprefix("dk:"))
        print(f"    {name:<24} ${r.SALARY:,}  {what}")


def export(scored: pd.DataFrame, week) -> pd.DataFrame:
    """The dashboard's 14 columns, named for display, plus player_id; saved to
    data/api_week_export.csv.

    Two columns show what the model reads rather than their old sources:
    SG_FORM is the model's form rating (SGA_TOTAL: field-strength adjusted,
    Korn Ferry included); OWGR_RANK is this week's world ranking (display only:
    the model does not read it)."""
    names = display_names()
    out = scored.copy()
    out["SG_FORM"] = out["SGA_TOTAL"]
    out["OWGR_RANK"] = out["OWGR_NOW_RANK"]
    out["player_id"] = out["PLAYER"]
    out["PLAYER"] = [names.get(p, p[3:] if str(p).startswith("dk:") else p) for p in out["PLAYER"]]
    out = out[[c for c in EXPORT_COLS if c in out.columns] + ["player_id"]]
    for c, nd in ROUNDING.items():
        if c in out:
            out[c] = out[c].round(nd)
    out = out.sort_values("P_TOP20", ascending=False).reset_index(drop=True)
    out.to_csv(EXPORT_CSV, index=False)
    week_marker(week)
    print(f"exported {len(out)} players to data/{EXPORT_CSV.name}")
    return out


def week_marker(week) -> None:
    import json
    names = legacy.course_names()
    meta = {"tournament_id": week.tournament_id, "name": week.name,
            "course": names.get(legacy.week_course_key(week.tournament_id), week.course),
            "season": week.season, "ending_date": str(week.end_date)}
    (DATA / "api_week.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")


# ---------------------------------------------------------------- the log

def _slug(name: str) -> str:
    import re
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def log_predictions(export_df: pd.DataFrame, week) -> Path | None:
    """Save this week's forecast to data/predictions/ (committed: it can only be
    made before the event). Every run before the first tee replaces the week's
    file, so the report card grades the last forecast made on pre-event data;
    from the first tee on the file is frozen, and none is started."""
    from pga_api import odds as api_odds
    PRED_DIR.mkdir(exist_ok=True)
    path = PRED_DIR / f"{week.season}-{week.end_date}-{_slug(week.name)}.csv"
    old = pd.read_csv(path, dtype={"player_id": str}) if path.exists() else None
    lock = api_odds.lock_time(week)
    if datetime.now(timezone.utc) >= lock:
        kept = f"keeping the forecast logged {old['PREDICTED_AT'].iloc[0]}" if old is not None \
            else "no forecast logged for this week"
        print(f"{week.name} teed off {lock:%a %b %d %H:%M} UTC: {kept}; nothing written.")
        return None
    cols = ["player_id", "PLAYER", "SALARY", "P_TOP20", "SCORE", "MODEL_SCORE", "ODDS_SHARE",
            "LEVERAGE", "VEGAS_ODDS", "SG_FORM"]
    df = export_df[[c for c in cols if c in export_df]].copy()
    df.insert(0, "tournament_id", week.tournament_id)
    if old is not None:
        # Round-trip through CSV so the comparison sees what the file would hold.
        fresh = pd.read_csv(io.StringIO(df.to_csv(index=False)), dtype={"player_id": str})
        if fresh.equals(old.drop(columns="PREDICTED_AT")):
            print(f"predictions for {week.name} unchanged since {old['PREDICTED_AT'].iloc[0]}; "
                  "file left as is.")
            return None
    df["PREDICTED_AT"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    df.to_csv(path, index=False)
    if old is None:
        print(f"logged {len(df)} predictions to data/predictions/{path.name}  (commit it)")
    else:
        m = df.merge(old, on="player_id", how="outer", suffixes=("", "_old"), indicator=True)
        moved = (m["P_TOP20"] - m["P_TOP20_old"]).abs()
        print(f"replaced the forecast logged {old['PREDICTED_AT'].iloc[0]} in "
              f"data/predictions/{path.name}  (commit it): P_TOP20 moved for "
              f"{int((moved > 0).sum())} players (largest {moved.max():.3f}), "
              f"{int((m['_merge'] == 'left_only').sum())} added, "
              f"{int((m['_merge'] == 'right_only').sum())} dropped.")
    return path


def logged_predictions() -> pd.DataFrame:
    """Every logged forecast: the golf.db log (the old notebook) and this one's."""
    with sqlite3.connect(build.DB_PATH) as con:
        old = pd.read_sql("SELECT * FROM predictions", con).assign(logged_by="golf.db notebook")
    files = sorted(PRED_DIR.glob("*.csv"))
    new = (pd.concat([pd.read_csv(f, dtype={"player_id": str}) for f in files], ignore_index=True)
           .rename(columns=str.lower).assign(logged_by="API notebook") if files else pd.DataFrame())
    return pd.concat([old, new], ignore_index=True)


def report_card(last_n: int = 10) -> pd.DataFrame:
    """Each logged week against its result: how many of the top 15 by P_TOP20
    finished top 20 (the forward test's yardstick), what P_TOP20 expected, and
    the top 15's cut rate. One row per notebook, so the two can be compared."""
    preds = logged_predictions()
    if preds.empty:
        print("No predictions logged yet.")
        return pd.DataFrame()
    with sqlite3.connect(build.DB_PATH) as con:
        res = pd.read_sql("SELECT tournament_id, player_id, position, finish_rank FROM results", con)
        ev = pd.read_sql("SELECT tournament_id, name, end_date FROM events", con)
    j = preds.merge(res, on=["tournament_id", "player_id"], how="left")
    rows = []
    for (tid, who), g in j.groupby(["tournament_id", "logged_by"]):
        top = g.nlargest(15, "p_top20")
        graded = g["position"].notna().any()
        rows.append({"tournament_id": tid, "logged_by": who,
                     "top15_in_top20": int((top["finish_rank"] <= 20).sum()) if graded else None,
                     "expected_top20": round(float(top["p_top20"].sum()), 1),
                     "top15_cut_rate": round(float((~top["position"].isin(["CUT", "W/D", "DQ"])).mean()), 2)
                     if graded else None,
                     "status": "graded" if graded else "awaiting results"})
    wk = pd.DataFrame(rows).merge(ev, on="tournament_id").sort_values("end_date", ascending=False)
    wk = wk[["end_date", "name", "logged_by", "top15_in_top20", "expected_top20",
             "top15_cut_rate", "status"]]
    recent = wk["end_date"].drop_duplicates().head(last_n)
    wk = wk[wk["end_date"].isin(recent)].reset_index(drop=True)
    for who, g in wk[wk["status"] == "graded"].groupby("logged_by"):
        print(f"{who}: {len(g)} graded week(s), top 15 -> top 20 hits {g['top15_in_top20'].mean():.2f} "
              f"(forward-test baseline about 6.5)")
    return wk
