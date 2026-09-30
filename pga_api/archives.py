"""Data the API cannot give back, brought into pga.db under the Tour's ids:
betting odds, DraftKings prices and the predictions logged before each event.
All of it is committed files, read again on every rebuild:

    data/history/golfdb_odds.csv         every golfodds.com board golf.db held
    data/history/golfdb_predictions.csv  the old notebook's forecast log
    data/odds/                           each week's saved board (weekly.odds)
    data/salaries/                       each week's DraftKings file

The two history files are golf.db's odds and predictions tables as they stood
when pga-dk.ipynb was retired (September 2026), names as golf.db spelled them.
They never change again; new weeks arrive as files in data/odds/.

Every name goes through identity.Resolver inside its event's field; how each
one resolved is kept in `name_resolution`, and unresolved names are never
guessed.
"""

from __future__ import annotations

import glob
import json
import os
from pathlib import Path

import pandas as pd

from pga_api.identity import Resolver

DATA = Path(__file__).resolve().parent.parent / "data"
HISTORY_DIR = DATA / "history"
SALARY_DIR = DATA / "salaries"
ODDS_DIR = DATA / "odds"
KEY = ["SEASON", "TOURNAMENT", "ENDING_DATE"]
_TEXT = {"TOURNAMENT": str, "ENDING_DATE": str, "PLAYER": str, "ODDS": str}


def name_keyed_source() -> dict[str, pd.DataFrame]:
    """The golfodds boards and forecasts that are keyed by name, not player id:
    golf.db's history, plus the golfodds boards weekly.odds saves to data/odds/
    (which win where both have an event)."""
    read = lambda f: pd.read_csv(f, dtype=_TEXT, float_precision="round_trip")
    odds = read(HISTORY_DIR / "golfdb_odds.csv").assign(origin="golf.db")
    pred = read(HISTORY_DIR / "golfdb_predictions.csv")
    files = sorted(glob.glob(str(ODDS_DIR / "golfodds-*.csv")))
    if files:
        saved = pd.concat([read(f).assign(origin="data/odds") for f in files], ignore_index=True)
        odds = pd.concat([saved, odds], ignore_index=True)
    return {"odds": odds, "predictions": pred}


def _resolve_events(R: Resolver, df: pd.DataFrame, name_col: str) -> pd.DataFrame:
    """One row per (SEASON, TOURNAMENT, ENDING_DATE) group with the Tour event it is."""
    rows = []
    for k, g in df.groupby(KEY):
        tid, share = R.match_event(k[2], g[name_col].tolist())
        rows.append(dict(zip(KEY, k), tournament_id=tid, name_share=round(share, 3)))
    return pd.DataFrame(rows)


def _resolve_names(R: Resolver, df: pd.DataFrame, name_col: str, source: str) -> pd.DataFrame:
    out = [R.resolve(n, t) if pd.notna(t) else (None, "no event")
           for n, t in zip(df[name_col], df["tournament_id"])]
    df = df.assign(player_id=[o[0] for o in out], how=[o[1] for o in out])
    df["source"] = source
    return df


def port_odds(R: Resolver, odds: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    ev = _resolve_events(R, odds, "PLAYER")
    o = odds.merge(ev, on=KEY)
    if "origin" in o:   # one board per event: a saved weekly file beats golf.db's copy
        first = o.dropna(subset=["tournament_id"]).groupby("tournament_id")["origin"].first()
        o = o[o["tournament_id"].isna() | (o["origin"] == o["tournament_id"].map(first))]
    o = _resolve_names(R, o, "PLAYER", "golfodds")
    audit = o[["source", "PLAYER", "tournament_id", "player_id", "how"]].rename(columns={"PLAYER": "name"})
    keep = o[o["player_id"].notna()]
    # One price per golfer per event: the same golfer listed twice keeps the first.
    keep = keep.drop_duplicates(["tournament_id", "player_id"])
    as_of = keep["SCRAPED_AT"] if "SCRAPED_AT" in keep else pd.Series(None, index=keep.index)
    table = pd.DataFrame({"tournament_id": keep["tournament_id"], "player_id": keep["player_id"],
                          "odds_text": keep["ODDS"], "decimal_minus_one": keep["VEGAS_ODDS"],
                          "book": "golfodds.com", "as_of": as_of.where(keep["origin"] == "data/odds")})
    return table, audit


def port_fanduel(directory: Path = ODDS_DIR) -> pd.DataFrame:
    """The FanDuel boards pga_api.weekly saved. Already keyed by player id:
    nothing to resolve. The latest save of a week wins."""
    files = sorted(glob.glob(str(directory / "fanduel-*.csv")))
    if not files:
        return pd.DataFrame(columns=ODDS_COLS)
    b = pd.concat([pd.read_csv(f, dtype={"player_id": str, "tournament_id": str, "odds_text": str}) for f in files],
                  ignore_index=True)
    b = b[b["AS_OF"] == b.groupby("tournament_id")["AS_OF"].transform("max")]
    return pd.DataFrame({"tournament_id": b["tournament_id"], "player_id": b["player_id"],
                         "odds_text": b["odds_text"], "decimal_minus_one": b["VEGAS_ODDS"],
                         "book": "FanDuel", "as_of": b["AS_OF"]}).drop_duplicates(["tournament_id", "player_id"])


ODDS_COLS = ["tournament_id", "player_id", "odds_text", "decimal_minus_one", "book", "as_of"]


def one_board_per_event(*tables: pd.DataFrame) -> pd.DataFrame:
    """Each event's odds from ONE board: the one saved last (as_of), whichever
    book it came from, so the board a forecast was made with is the board the
    database keeps. golf.db's history (no as_of) only fills events no weekly
    board was saved for."""
    odds = pd.concat([t for t in tables if len(t)], ignore_index=True)
    when = pd.to_datetime(odds["as_of"], utc=True, format="ISO8601")
    board = odds["book"] + "|" + odds["as_of"].fillna("")
    latest = (odds.assign(when=when, board=board).sort_values("when", na_position="first")
              .groupby("tournament_id")["board"].last())
    return odds[board == odds["tournament_id"].map(latest)].reset_index(drop=True)[ODDS_COLS]


def port_predictions(R: Resolver, pred: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if pred.empty:
        return pd.DataFrame(), pd.DataFrame()
    ev = _resolve_events(R, pred, "PLAYER")
    p = _resolve_names(R, pred.merge(ev, on=KEY), "PLAYER", "predictions")
    audit = p[["source", "PLAYER", "tournament_id", "player_id", "how"]].rename(columns={"PLAYER": "name"})
    cols = [c for c in pred.columns if c not in KEY + ["PLAYER"]]
    table = p[p["player_id"].notna()][["tournament_id", "player_id"] + cols]
    table.columns = ["tournament_id", "player_id"] + [c.lower() for c in cols]
    return table, audit


def port_salaries(R: Resolver, directory: Path = SALARY_DIR,
                  only: str | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    frames, audits = [], []
    for path in sorted(glob.glob(str(directory / (only or "dk-*.csv")))):
        df = pd.read_csv(path, encoding="utf-8-sig")
        meta_path = path[:-4] + "-meta.json"
        meta = json.load(open(meta_path, encoding="utf-8")) if os.path.exists(meta_path) else {}
        end = meta.get("config_ending_date") or "-".join(Path(path).stem.split("-")[2:5])
        tid, share = R.match_event(end, df["Name"].tolist())
        df["tournament_id"] = tid
        df = _resolve_names(R, df, "Name", "draftkings")
        audits.append(df[["source", "Name", "tournament_id", "player_id", "how"]].rename(columns={"Name": "name"}))
        frames.append(pd.DataFrame({
            "tournament_id": df["tournament_id"], "player_id": df["player_id"],
            "dk_player_id": df["ID"].astype(str), "dk_name": df["Name"], "salary": df["Salary"],
            "avg_points": df["AvgPointsPerGame"], "status": df.get("Status"),
            "draft_group": meta.get("draft_group"), "file": Path(path).name}))
    if not frames:
        return pd.DataFrame(), pd.DataFrame()
    return pd.concat(frames, ignore_index=True), pd.concat(audits, ignore_index=True)


def port_all(frames: dict, fields: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    R = Resolver(frames["events"], frames["results"], frames["players"], fields=fields)
    src = name_keyed_source()
    golfodds, a1 = port_odds(R, src["odds"])
    odds = one_board_per_event(golfodds, port_fanduel())
    pred, a2 = port_predictions(R, src["predictions"])
    sal, a3 = port_salaries(R)
    audit = (pd.concat([a1, a2, a3], ignore_index=True)
             .groupby(["source", "name", "tournament_id", "player_id", "how"], dropna=False)
             .size().rename("rows").reset_index())
    return {"odds": odds, "predictions": pred, "dk_salaries": sal, "name_resolution": audit}
