"""pga.db in the shape utils/features.py reads: the tables load_tables() returns
from golf.db, rebuilt from the API with the Tour's ids in place of names.

    t, s, o = legacy.tables()     # tournaments, stats, odds
    utils.features.build_event_rows(t, s, o, event, ...)   # unchanged code

So the features and the model are the same code in both pipelines; anything
that differs between them is the data. Columns keep golf.db's names:

    PLAYER      player_id ("46046"), never a name
    TOURNAMENT  tournament_id ("R2026013")
    COURSE      the host course's id ("776"), so course history no longer hangs
                on how a course was spelled that year
"""

from __future__ import annotations

import re
import sqlite3

import numpy as np
import pandas as pd

from pga_api import build

STATS = list(build.STAT_IDS)
MODEL_KINDS = ("stroke",)


def _read(con, table: str) -> pd.DataFrame:
    return pd.read_sql(f"SELECT * FROM {table}", con)


def _name_key(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(s).lower())


_FILLER = {"the", "golf", "club", "course", "resort", "and", "spa", "links", "gc", "g"}


def _course_name_key(s: str) -> str:
    """The distinctive words only: 'Torrey Pines Golf Course (South Course)' and
    'Torrey Pines (South)' both -> 'torreypinessouth'. North/South/Seaside stay,
    so two courses at one club stay two."""
    s = re.sub(r"country\s+club", "cc", str(s).lower())
    words = re.findall(r"[a-z0-9]+", s)
    return "".join(w for w in words if w not in _FILLER)


def course_keys(events: pd.DataFrame, courses: pd.DataFrame) -> pd.Series:
    """tournament_id -> one key per physical course.

    The Tour's course ids are not stable (Quail Hollow is 872 in 2018 and 241 in
    2025; Muirfield Village has one id at the Memorial and another at the 2020
    Workday), and names drift the other way (Sawgrass keeps 011 through a
    rewording). So a course is every id and every name linked to it by any
    event: same id or same name means same course, transitively. An event not
    yet played has no course list, and joins through its schedule name."""
    host = courses[courses["host"].astype(bool)].drop_duplicates("tournament_id")
    nodes = pd.DataFrame({"tournament_id": events["tournament_id"].values,
                          "name": events["course"].map(_course_name_key).values})
    nodes = nodes.merge(host[["tournament_id", "course_id", "course_name"]], on="tournament_id", how="left")
    nodes["host_name"] = [_course_name_key(n) if isinstance(n, str) else "" for n in nodes["course_name"]]

    parent: dict = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        parent[find(a)] = find(b)

    for r in nodes.itertuples():
        labels = [f"n:{r.name}"]
        if pd.notna(r.course_id):
            labels.append(f"i:{r.course_id}")
        if r.host_name:
            labels.append(f"n:{r.host_name}")
        for x in labels[1:]:
            union(labels[0], x)
    keys = [find(f"n:{n}") for n in nodes["name"]]
    return pd.Series(keys, index=nodes["tournament_id"].values)


def week_course_key(tournament_id: str, db_path=build.DB_PATH) -> str:
    """The course key of one event, played or not, in the same key space as
    tables()'s COURSE column."""
    with sqlite3.connect(db_path) as con:
        ev = _read(con, "events")
        courses = _read(con, "event_courses")
    ev = ev[ev["kind"].isin(MODEL_KINDS) | (ev["tournament_id"] == tournament_id)]
    return course_keys(ev, courses)[tournament_id]


def course_names(db_path=build.DB_PATH) -> pd.Series:
    """course key -> the name to show for it: its most recent host course name."""
    with sqlite3.connect(db_path) as con:
        ev = _read(con, "events")
        courses = _read(con, "event_courses")
    ev = ev[ev["kind"].isin(MODEL_KINDS) | (ev["kind"] == "upcoming")].sort_values("end_date")
    keys = course_keys(ev, courses)
    host = courses[courses["host"].astype(bool)].drop_duplicates("tournament_id").set_index("tournament_id")["course_name"]
    shown = ev["tournament_id"].map(host).fillna(ev["course"])
    return pd.Series(shown.values, index=keys[ev["tournament_id"]].values).groupby(level=0).last()


def _round_values(rounds: pd.DataFrame) -> pd.DataFrame:
    """One number per round, in the unit that is comparable within that
    event-round: score to par when every golfer has it (a multi-course round is
    then fair across courses of different par), strokes otherwise."""
    r = rounds.copy()
    full = r.groupby(["tournament_id", "round"])["to_par"].transform(lambda s: s.notna().all())
    r["value"] = np.where(full, r["to_par"], r["strokes"])
    return r


def tables(db_path=build.DB_PATH, kinds=MODEL_KINDS):
    """-> (t, s, o) exactly as utils.features.load_tables returns them."""
    with sqlite3.connect(db_path) as con:
        ev = _read(con, "events")
        res = _read(con, "results")
        rnd = _read(con, "rounds")
        st = _read(con, "season_stats")
        odds = _read(con, "odds")
        courses = _read(con, "event_courses")

    ev = ev[ev["kind"].isin(kinds)].copy()
    ev["COURSE"] = ev["tournament_id"].map(course_keys(ev, courses))
    t = res.merge(ev[["tournament_id", "season", "end_date", "COURSE"]], on="tournament_id")

    wide = (_round_values(rnd).pivot_table(index=["tournament_id", "player_id"], columns="round",
                                           values="value", aggfunc="first"))
    wide.columns = [f"ROUNDS:{int(c)}" for c in wide.columns]
    t = t.merge(wide.reset_index(), on=["tournament_id", "player_id"], how="left")
    for i in (1, 2, 3, 4):
        if f"ROUNDS:{i}" not in t:
            t[f"ROUNDS:{i}"] = np.nan

    t = pd.DataFrame({
        "SEASON": t["season"].astype(int),
        "ENDING_DATE": pd.to_datetime(t["end_date"]),
        "TOURN_ID": t["tournament_id"],
        "TOURNAMENT": t["tournament_id"],
        "COURSE": t["COURSE"],
        "PLAYER": t["player_id"],
        "POS": t["position"],
        # golf.db's convention: a finish's number, 90 for CUT / W/D / DQ / MDF.
        "FINAL_POS": t["finish_rank"].fillna(90).astype(float),
        **{f"ROUNDS:{i}": t[f"ROUNDS:{i}"] for i in (1, 2, 3, 4)},
        "OFFICIAL_MONEY": t["official_money"],
        "FEDEX_CUP_POINTS": t["fedex_points"],
    })
    field_n = t.groupby(["TOURNAMENT", "ENDING_DATE"])["PLAYER"].transform("size")
    t["FINISH_PCT"] = np.minimum(t["FINAL_POS"], field_n) / field_n

    s = st.pivot_table(index=["season", "player_id"], columns="stat", values="value", aggfunc="first")
    r = st.pivot_table(index=["season", "player_id"], columns="stat", values="rank", aggfunc="first")
    r.columns = [f"{c}_RANK" for c in r.columns]
    s = s.join(r).reset_index().rename(columns={"season": "SEASON", "player_id": "PLAYER"})
    for c in STATS + [f"{c}_RANK" for c in STATS]:
        if c not in s:
            s[c] = np.nan

    o = odds.merge(ev[["tournament_id", "season", "end_date"]], on="tournament_id")
    o = pd.DataFrame({"SEASON": o["season"].astype(int), "TOURNAMENT": o["tournament_id"],
                      "ENDING_DATE": pd.to_datetime(o["end_date"]), "PLAYER": o["player_id"],
                      "VEGAS_ODDS": o["decimal_minus_one"]})
    o = o.drop_duplicates(subset=["TOURNAMENT", "ENDING_DATE", "PLAYER"], keep="first")
    return t, s, o
