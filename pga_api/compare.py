"""Parity check: data/golf.db (hand-maintained) against data/pga.db (rebuilt from the API).

Reads both, writes neither. Every difference lands in one table of `report()`'s
result, so each can be looked at on its own:

    from pga_api import compare
    r = compare.report()
    r["rounds"].head()
"""

from __future__ import annotations

import sqlite3
from difflib import SequenceMatcher
from pathlib import Path

import numpy as np
import pandas as pd

from utils.db_utils import PLAYER_NAME_MAP, normalize_name

DATA = Path(__file__).resolve().parent.parent / "data"
GOLF_DB = DATA / "golf.db"
PGA_DB = DATA / "pga.db"

STAT_COLS = ["SGTTG", "SGOTT", "SGAPR", "SGATG", "SGP", "BIRDIES", "PAR_3", "PAR_4", "PAR_5",
             "TOTAL_DRIVING", "DRIVING_DISTANCE", "DRIVING_ACCURACY", "GIR", "SCRAMBLING", "OWGR"]


def std_name(name: str) -> str:
    """The spelling golf.db stores: what utils.db_utils.standardize_player_names makes."""
    n = normalize_name(name)
    return PLAYER_NAME_MAP.get(n, n)


def _num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.astype(str).str.replace(r"[$,%]", "", regex=True)
                         .replace({"E": "0", "None": None, "nan": None, "": None}), errors="coerce")


def _load():
    with sqlite3.connect(GOLF_DB) as g:
        gt = pd.read_sql("SELECT * FROM tournaments", g)
        gs = pd.read_sql("SELECT * FROM stats", g)
    with sqlite3.connect(PGA_DB) as p:
        ev = pd.read_sql("SELECT * FROM events", p)
        res = pd.read_sql("SELECT * FROM results", p)
        rnd = pd.read_sql("SELECT * FROM rounds", p)
        pl = pd.read_sql("SELECT * FROM players", p)
        st = pd.read_sql("SELECT * FROM season_stats", p)
    pl["std"] = pl["name"].map(std_name)
    return gt, gs, ev, res, rnd, pl, st


def _similar(a: str, b: str) -> float:
    return SequenceMatcher(None, str(a).lower(), str(b).lower()).ratio()


def match_events(gt: pd.DataFrame, ev: pd.DataFrame) -> pd.DataFrame:
    """One row per golf.db event, with the API event it is and how they differ.

    Matched by DATE, not by golf.db's TOURN_ID: that column was typed by hand and
    several events carry another year's id. Among API events ending within 4 days,
    the same tournament number wins, then the closest name.
    """
    g = (gt.groupby(["SEASON", "ENDING_DATE", "TOURN_ID", "TOURNAMENT", "COURSE"], dropna=False)
           .size().rename("golf_rows").reset_index())
    tid = g["TOURN_ID"].astype(str)
    g["golf_id"] = np.where(tid.str.len() == 8, tid, "R" + g["SEASON"].astype(str) + tid.str[-3:])
    api = ev[ev["kind"] != "upcoming"].copy()
    api["end"] = pd.to_datetime(api["end_date"])
    chosen = []
    for _, row in g.iterrows():
        end = pd.to_datetime(row["ENDING_DATE"])
        c = api[(api["end"] - end).abs() <= pd.Timedelta(days=4)]
        if c.empty:
            chosen.append(None)
            continue
        same_no = c[c["tournament_id"].str[-3:] == str(row["TOURN_ID"])[-3:]]
        if len(same_no) == 1:
            chosen.append(same_no["tournament_id"].iloc[0])
            continue
        score = c["name"].map(lambda n: _similar(n, row["TOURNAMENT"])) \
            + c["course"].map(lambda n: _similar(n, row["COURSE"]))
        chosen.append(c.loc[score.idxmax(), "tournament_id"])
    g["tournament_id"] = chosen
    m = g.merge(ev[["tournament_id", "name", "course", "end_date", "kind", "source", "field_size"]],
                on="tournament_id", how="left")
    m["found"] = m["name"].notna()
    m["id_wrong"] = m["found"] & (m["golf_id"] != m["tournament_id"])
    # One API event claimed by two golf.db events: at most one of them is right.
    m["claimed_twice"] = m["found"] & m.duplicated("tournament_id", keep=False)
    m["end_diff_days"] = (pd.to_datetime(m["ENDING_DATE"]) - pd.to_datetime(m["end_date"])).dt.days
    m["row_diff"] = m["golf_rows"] - m["field_size"]
    m["course_differs"] = m["found"] & (m["COURSE"] != m["course"])
    m["name_differs"] = m["found"] & (m["TOURNAMENT"] != m["name"])
    return m


def missing_events(ev: pd.DataFrame, m: pd.DataFrame) -> pd.DataFrame:
    """Stroke-play events the API has for golf.db's seasons that golf.db lacks."""
    seasons = m["SEASON"].unique()
    have = set(m.loc[m["found"], "tournament_id"])
    out = ev[(ev["kind"] == "stroke") & ev["season"].isin(seasons) & ~ev["tournament_id"].isin(have)]
    return out[["season", "tournament_id", "name", "course", "end_date", "source", "field_size"]]


def match_players(gt, m, res, pl):
    """golf.db rows joined to API results within each matched event, by standardized name.

    -> (joined rows, golf.db rows with no API player, API players with no golf.db row)
    """
    key = m[m["found"]][["SEASON", "ENDING_DATE", "TOURNAMENT", "tournament_id", "source"]]
    g = gt.merge(key, on=["SEASON", "ENDING_DATE", "TOURNAMENT"])
    a = res[res["tournament_id"].isin(key["tournament_id"])].merge(
        pl[["player_id", "name", "std"]], on="player_id")
    j = g.merge(a, left_on=["tournament_id", "PLAYER"], right_on=["tournament_id", "std"],
                how="outer", indicator=True)
    both = j[j["_merge"] == "both"].copy()
    golf_only = j[j["_merge"] == "left_only"][list(gt.columns) + ["tournament_id"]]
    api_only = j[j["_merge"] == "right_only"][["tournament_id", "player_id", "name", "position"]]
    return both, golf_only, api_only


def pair_unmatched(golf_only, api_only, rnd):
    """Pair golf.db-only and API-only rows in the same event whose rounds agree:
    the same golfer under two spellings."""
    wide = rnd.pivot_table(index=["tournament_id", "player_id"], columns="round",
                           values=["strokes", "to_par"]).reset_index()
    wide.columns = ["tournament_id", "player_id"] + [f"{a}_{b}" for a, b in wide.columns[2:]]
    a = api_only.merge(wide, on=["tournament_id", "player_id"], how="left")
    pairs = []
    for tid, gg in golf_only.groupby("tournament_id"):
        aa = a[a["tournament_id"] == tid]
        for _, row in gg.iterrows():
            for _, cand in aa.iterrows():
                if all(_same_round(row[f"ROUNDS:{i}"], cand.get(f"strokes_{i}"), cand.get(f"to_par_{i}"))
                       for i in (1, 2)):
                    pairs.append({"tournament_id": tid, "golf_name": row["PLAYER"],
                                  "api_name": cand["name"], "player_id": cand["player_id"]})
    return pd.DataFrame(pairs, columns=["tournament_id", "golf_name", "api_name", "player_id"])


def _same_round(golf_val, strokes, to_par) -> bool:
    v = _num(pd.Series([golf_val])).iloc[0]
    if pd.isna(v):
        return False
    s = str(golf_val).strip()
    is_to_par = s.startswith(("+", "-")) or s == "E" or abs(v) < 30
    return v == (to_par if is_to_par else strokes)


def compare_rows(both: pd.DataFrame, rnd: pd.DataFrame) -> dict:
    """Field-by-field differences on rows present in both."""
    out = {}
    pos_g = both["POS"].astype(str).str.strip()
    pos_a = both["position"].astype(str).str.strip()
    out["position"] = both.loc[pos_g != pos_a,
                               ["tournament_id", "TOURNAMENT", "SEASON", "PLAYER", "POS", "position"]]

    wide = rnd.pivot_table(index=["tournament_id", "player_id"], columns="round",
                           values=["strokes", "to_par"]).reset_index()
    wide.columns = ["tournament_id", "player_id"] + [f"{a}_{b}" for a, b in wide.columns[2:]]
    b = both.merge(wide, on=["tournament_id", "player_id"], how="left")
    diffs = []
    for i in (1, 2, 3, 4):
        raw = b[f"ROUNDS:{i}"]
        gv = _num(raw)
        s = raw.astype(str).str.strip()
        to_par_fmt = s.str.startswith(("+", "-")) | (s == "E") | (gv.abs() < 30)
        av = np.where(to_par_fmt, b.get(f"to_par_{i}"), b.get(f"strokes_{i}"))
        av = pd.to_numeric(pd.Series(av, index=b.index), errors="coerce")
        bad = ~((gv.isna() & av.isna()) | (gv == av))
        d = b.loc[bad, ["tournament_id", "TOURNAMENT", "SEASON", "PLAYER"]].copy()
        d["round"], d["golf"], d["api"] = i, raw[bad], av[bad]
        d["format"] = np.where(to_par_fmt[bad], "to_par", "strokes")
        diffs.append(d)
    out["rounds"] = pd.concat(diffs, ignore_index=True)

    # golf.db rounds money to the dollar and leaves a missed cut blank where the API
    # says 0; neither is a disagreement. A value only golf.db has (the leaderboard
    # fallback carries no points or money) is kept apart from one that differs.
    for gcol, acol, name, tol in (("FEDEX_CUP_POINTS", "fedex_points", "fedex", 0.01),
                                  ("OFFICIAL_MONEY", "official_money", "money", 1.0)):
        gv, av = _num(both[gcol]), pd.to_numeric(both[acol], errors="coerce")
        cols = ["tournament_id", "TOURNAMENT", "SEASON", "PLAYER", gcol, acol]
        api_blank = av.isna() & (both["source"] == "leaderboard")
        out[f"{name}_golf_only"] = both.loc[api_blank & gv.notna(), cols]
        g0, a0 = gv.fillna(0), av.fillna(0)
        out[name] = both.loc[~api_blank & ((g0 - a0).abs() >= tol), cols]
    return out


def compare_stats(gs: pd.DataFrame, st: pd.DataFrame, pl: pd.DataFrame) -> dict:
    a = st.merge(pl[["player_id", "std"]], on="player_id")
    a = a.pivot_table(index=["season", "std"], columns="stat", values="value", aggfunc="first")
    g = gs.copy()
    for c in STAT_COLS:
        g[c] = _num(g[c])
    g = g.set_index(["SEASON", "PLAYER"])[STAT_COLS]
    g.index.names = ["season", "std"]
    seasons = g.index.get_level_values(0).unique()
    a = a[a.index.get_level_values(0).isin(seasons)]
    j = g.join(a, how="outer", lsuffix="_golf", rsuffix="_api")
    rows = []
    for c in STAT_COLS:
        gv, av = j[f"{c}_golf"], j[f"{c}_api"]
        rows.append({"stat": c, "both": int((gv.notna() & av.notna()).sum()),
                     "golf_only": int((gv.notna() & av.isna()).sum()),
                     "api_only": int((gv.isna() & av.notna()).sum()),
                     "value_differs": int((gv.notna() & av.notna() & ((gv - av).abs() > 0.011)).sum())})
    return {"stats_summary": pd.DataFrame(rows), "stats_joined": j}


def report(verbose: bool = True) -> dict:
    gt, gs, ev, res, rnd, pl, st = _load()
    m = match_events(gt, ev)
    miss = missing_events(ev, m)
    both, golf_only, api_only = match_players(gt, m, res, pl)
    pairs = pair_unmatched(golf_only, api_only, rnd)
    rows = compare_rows(both, rnd)
    stats = compare_stats(gs, st, pl)
    out = {"events": m, "missing_events": miss, "golf_only": golf_only, "api_only": api_only,
           "name_pairs": pairs, **rows, **stats}
    if verbose:
        _print(out, gt)
    return out


def _print(r: dict, gt: pd.DataFrame) -> None:
    m = r["events"]
    print(f"golf.db: {len(gt):,} rows in {len(m)} events")
    print(f"  events with no API event that week: {int((~m['found']).sum())}")
    print(f"  golf.db TOURN_ID is another event's: {int(m['id_wrong'].sum())}")
    print(f"  two golf.db events, one API event:  {int(m['claimed_twice'].sum())}")
    print(f"  end date differs:                   {int((m['end_diff_days'].fillna(0) != 0).sum())}")
    print(f"  row count differs from API field:   {int((m['row_diff'].fillna(0) != 0).sum())}")
    print(f"  course spelled differently:         {int(m['course_differs'].sum())}")
    print(f"stroke-play events golf.db lacks:     {len(r['missing_events'])}")
    print(f"player rows only in golf.db:          {len(r['golf_only']):,}"
          f"   (of which paired to an API spelling by score: {len(r['name_pairs']):,})")
    print(f"player rows only in the API:          {len(r['api_only']):,}")
    for k in ("position", "rounds", "fedex", "money"):
        print(f"{k:<10} differences on matched rows: {len(r[k]):,}")
    print(f"fedex points only golf.db has:        {len(r['fedex_golf_only']):,}")
    print("season stats:")
    print(r["stats_summary"].to_string(index=False))
