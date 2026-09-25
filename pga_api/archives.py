"""Data the API cannot give back, brought into pga.db under the Tour's ids:
betting odds, DraftKings prices and the predictions logged before each event.

Odds and predictions are read from golf.db while pga-dk.ipynb still writes
them there each week (`golf_db_source`); once the new notebook owns the weekly
run they move to committed files, as data/salaries/ already is.

Every name goes through identity.Resolver inside its event's field; how each
one resolved is kept in `name_resolution`, and unresolved names are never
guessed.
"""

from __future__ import annotations

import glob
import json
import os
import sqlite3
from pathlib import Path

import pandas as pd

from pga_api.identity import Resolver
from utils.db_utils import DK_PLAYER_NAME_MAP

DATA = Path(__file__).resolve().parent.parent / "data"
GOLF_DB = DATA / "golf.db"
SALARY_DIR = DATA / "salaries"
KEY = ["SEASON", "TOURNAMENT", "ENDING_DATE"]


def golf_db_source() -> dict[str, pd.DataFrame]:
    with sqlite3.connect(GOLF_DB) as con:
        return {"odds": pd.read_sql("SELECT * FROM odds", con),
                "predictions": pd.read_sql("SELECT * FROM predictions", con)}


def _resolve_events(R: Resolver, df: pd.DataFrame, name_col: str) -> pd.DataFrame:
    """One row per (SEASON, TOURNAMENT, ENDING_DATE) group with the Tour event it is."""
    rows = []
    for k, g in df.groupby(KEY):
        tid, share = R.match_event(k[2], g[name_col].tolist())
        rows.append(dict(zip(KEY, k), tournament_id=tid, name_share=round(share, 3)))
    return pd.DataFrame(rows)


def _resolve_names(R: Resolver, df: pd.DataFrame, name_col: str, source: str,
                   alt_names: dict | None = None) -> pd.DataFrame:
    out = [R.resolve(n, t) if pd.notna(t) else (None, "no event")
           for n, t in zip(df[name_col], df["tournament_id"])]
    df = df.assign(player_id=[o[0] for o in out], how=[o[1] for o in out])
    if alt_names:   # DraftKings spellings golf.db already mapped by hand
        miss = df["player_id"].isna() & df[name_col].isin(alt_names) & df["tournament_id"].notna()
        alt = [R.resolve(alt_names[n], t) for n, t in zip(df.loc[miss, name_col], df.loc[miss, "tournament_id"])]
        df.loc[miss, "player_id"] = [a[0] for a in alt]
        df.loc[miss, "how"] = ["dk-map+" + a[1] for a in alt]
    df["source"] = source
    return df


def port_odds(R: Resolver, odds: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    ev = _resolve_events(R, odds, "PLAYER")
    o = odds.merge(ev, on=KEY)
    o = _resolve_names(R, o, "PLAYER", "golfodds")
    audit = o[["source", "PLAYER", "tournament_id", "player_id", "how"]].rename(columns={"PLAYER": "name"})
    keep = o[o["player_id"].notna()]
    # One price per golfer per event: the same golfer listed twice keeps the first.
    keep = keep.drop_duplicates(["tournament_id", "player_id"])
    table = pd.DataFrame({"tournament_id": keep["tournament_id"], "player_id": keep["player_id"],
                          "odds_text": keep["ODDS"], "decimal_minus_one": keep["VEGAS_ODDS"],
                          "book": "golfodds.com"})
    return table, audit


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


def port_salaries(R: Resolver, directory: Path = SALARY_DIR) -> tuple[pd.DataFrame, pd.DataFrame]:
    frames, audits = [], []
    for path in sorted(glob.glob(str(directory / "dk-*.csv"))):
        df = pd.read_csv(path, encoding="utf-8-sig")
        meta_path = path[:-4] + "-meta.json"
        meta = json.load(open(meta_path, encoding="utf-8")) if os.path.exists(meta_path) else {}
        end = meta.get("config_ending_date") or "-".join(Path(path).stem.split("-")[2:5])
        tid, share = R.match_event(end, df["Name"].tolist())
        df["tournament_id"] = tid
        df = _resolve_names(R, df, "Name", "draftkings", alt_names=DK_PLAYER_NAME_MAP)
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
    src = golf_db_source()
    odds, a1 = port_odds(R, src["odds"])
    pred, a2 = port_predictions(R, src["predictions"])
    sal, a3 = port_salaries(R)
    audit = (pd.concat([a1, a2, a3], ignore_index=True)
             .groupby(["source", "name", "tournament_id", "player_id", "how"], dropna=False)
             .size().rename("rows").reset_index())
    return {"odds": odds, "predictions": pred, "dk_salaries": sal, "name_resolution": audit}
