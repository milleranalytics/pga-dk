"""The weekly routine's steps, one function each, for pga-weekly.ipynb.

Nothing here asks for a tournament name, date, course or id: this week's event
comes from the Tour's schedule, last week's results from the rebuild, and every
name is resolved to the Tour's player_id where it enters.
"""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from pga_api import build
from pga_api.identity import ALIASES, Resolver

DATA = Path(__file__).resolve().parent.parent / "data"
ODDS_DIR = DATA / "odds"
FIRST_SEASON = 2015


def _read(table: str) -> pd.DataFrame:
    with sqlite3.connect(build.DB_PATH) as con:
        return pd.read_sql(f"SELECT * FROM {table}", con)


def resolver() -> Resolver:
    field_ = _read("field")
    return Resolver(_read("events"), _read("results"), _read("players"),
                    fields={t: g for t, g in field_.groupby("tournament_id")})


# ---------------------------------------------------------------- 1. refresh

def refresh() -> pd.DataFrame:
    """Rebuild pga.db. Returns the events that finished since the last build."""
    before = set()
    if build.DB_PATH.exists():
        before = set(_read("events").query("completed == 1")["tournament_id"])
    season = date.today().year
    frames = build.build(range(FIRST_SEASON, season + 1),
                         stat_seasons=range(FIRST_SEASON - 1, season + 1), verbose=False)
    ev = frames["events"]
    new = ev[ev["completed"] & ~ev["tournament_id"].isin(before)]
    print(f"pga.db rebuilt: {int(ev['completed'].sum())} finished events, "
          f"{len(frames['results']):,} finishes, {len(frames['rounds']):,} rounds.")
    if before and len(new):
        for e in new.itertuples():
            print(f"  new since last run: {e.name} ({e.end_date}), {e.field_size} players, {e.kind}")
    elif before:
        print("  nothing new has finished since the last run.")
    return new[["tournament_id", "name", "end_date", "kind", "field_size"]]


# ---------------------------------------------------------------- 2. this week

@dataclass
class Week:
    tournament_id: str
    name: str
    course: str
    season: int
    start_date: date
    end_date: date
    others: list = field(default_factory=list)

    @property
    def config(self) -> dict:
        """The `tournament_config` shape utils/dk_api.py reads."""
        return {"new": {"name": self.name, "course": self.course, "season": self.season,
                        "ending_date": pd.Timestamp(self.end_date)}}


def this_week(pick: str | None = None, within_days: int = 7) -> Week:
    """The next event to start. When two share a week, the one with more of the
    world's top 50 in its field: the main event, not the opposite-field one.
    `pick="R2026554"` overrides."""
    ev = _read("events")
    ev["start_date"] = pd.to_datetime(ev["start_date"]).dt.date
    ev["end_date"] = pd.to_datetime(ev["end_date"]).dt.date
    today = date.today()
    cands = ev[(ev["kind"] == "upcoming") & (ev["start_date"] <= today + timedelta(days=within_days))
               & (ev["end_date"] >= today)]
    # An upcoming event has no format yet; its past editions do. The Presidents
    # Cup's strong field would otherwise win the week with no DraftKings slate.
    num = ev["tournament_id"].str[-3:]
    past = ev[ev["kind"] != "upcoming"].assign(num=num).sort_values("end_date").groupby("num")["kind"].last()
    cands = cands[cands["tournament_id"].str[-3:].map(past).fillna("stroke") == "stroke"]
    if pick:
        cands = ev[ev["tournament_id"] == pick]
    if cands.empty:
        raise LookupError(f"No event starts in the next {within_days} days. "
                          "Run refresh() first, or pass pick=<tournament id>.")
    f = _read("field")
    strength = (f[(f["entry"] == "field") & ~f["withdrawn"].astype(bool)]
                .assign(top50=lambda d: d["owgr"].fillna(999) <= 50)
                .groupby("tournament_id").agg(entries=("player_id", "size"), top50=("top50", "sum")))
    cands = cands.join(strength, on="tournament_id").fillna({"entries": 0, "top50": 0})
    cands = cands.sort_values(["top50", "entries"], ascending=False)
    main = cands.iloc[0]
    week = Week(main["tournament_id"], main["name"], main["course"], int(main["season"]),
                main["start_date"], main["end_date"],
                others=cands.iloc[1:][["tournament_id", "name", "top50", "entries"]].to_dict("records"))
    print(f"This week: {week.name} at {week.course}, {week.start_date:%a %b %d} to "
          f"{week.end_date:%a %b %d}  ({week.tournament_id})")
    print(f"  entry list: {int(main['entries'])} players, {int(main['top50'])} of the world's top 50")
    for o in week.others:
        print(f"  also this week: {o['name']} ({int(o['entries'])} players, "
              f"{int(o['top50'])} of the top 50); pass pick='{o['tournament_id']}' to use it")
    return week


# ---------------------------------------------------------------- 3. prices

def prices(week: Week) -> pd.DataFrame:
    """This week's saved DraftKings file, every name resolved to a player_id.
    No request to DraftKings: it reads data/salaries/ only."""
    from pga_api.archives import port_salaries
    from utils import dk_api
    try:
        path = Path(dk_api.find_archive(week.config))
    except FileNotFoundError:
        # Never another week's file: its prices would look entirely normal.
        raise FileNotFoundError(
            f"No DraftKings prices saved for {week.name} yet. Run 4a at home once "
            f"DraftKings posts the slate (usually early in the week), commit "
            f"data/salaries/, then run this cell again.") from None
    table, audit = port_salaries(resolver(), path.parent, only=path.name)
    table = table.merge(audit[["name", "how"]].rename(columns={"name": "dk_name"}), on="dk_name")
    wrong_event = table["tournament_id"].ne(week.tournament_id).any()
    print(f"DraftKings: {len(table)} priced players from {path.name}")
    if wrong_event:
        print(f"  !! the file's names match {table['tournament_id'].iloc[0]}, not {week.tournament_id}")
    _report_unresolved(table.rename(columns={"dk_name": "name"}), "DraftKings")
    return table


# ---------------------------------------------------------------- 4. odds

def odds(week: Week, source: str = "fanduel", save: bool = True) -> pd.DataFrame:
    """This week's win odds -> player_id, VEGAS_ODDS (12/1 -> 12.0), saved to
    data/odds/ (committed: a board cannot be fetched again for the time it was
    taken). source='fanduel' is the Tour's feed, keyed by player id: no names
    to fix and no board to mistake for another event. source='golfodds' is the
    scrape the old notebook uses."""
    if source == "fanduel":
        return _fanduel(week, save)
    if source != "golfodds":
        raise ValueError("source is 'fanduel' or 'golfodds'")
    return _golfodds(week, save)


def _save_board(board: pd.DataFrame, week: Week, prefix: str) -> None:
    ODDS_DIR.mkdir(exist_ok=True)
    slug = re.sub(r"[^a-z0-9]+", "-", week.name.lower()).strip("-")
    path = ODDS_DIR / f"{prefix}-{week.season}-{week.end_date}-{slug}.csv"
    board.to_csv(path, index=False)
    print(f"odds: {len(board)} prices saved to data/odds/{path.name}  (commit it)")


def _fanduel(week: Week, save: bool) -> pd.DataFrame:
    from pga_api import odds as api_odds
    board = api_odds.this_week(week)
    if board.empty or board["fraction"].isna().all():
        print("No FanDuel prices for this event yet: the market usually opens early in "
              "the week. Run this cell again later.")
        return pd.DataFrame(columns=["player_id", "VEGAS_ODDS"])
    board = board.dropna(subset=["fraction"]).rename(columns={"fraction": "VEGAS_ODDS"})
    board["AS_OF"] = board.attrs["as_of"]
    board["tournament_id"] = week.tournament_id
    print(f"FanDuel: {len(board)} golfers priced as of {board.attrs['as_of']} "
          f"(first tee {board.attrs['lock']:%a %H:%M} UTC)")
    fav = board.nsmallest(3, "VEGAS_ODDS")
    print("  favourites: " + ", ".join(f"{r.name} {r.odds_text}" for r in fav.itertuples()))
    if save:
        _save_board(board, week, "fanduel")
    return board


def _golfodds(week: Week, save: bool) -> pd.DataFrame:
    """Scrape golfodds.com, check the board is THIS event by its names, save, resolve."""
    from utils.db_utils import get_current_week_odds
    board = get_current_week_odds(season=week.season, tournament_name=week.name)
    if board.empty:
        return board
    R = resolver()
    f = R.field(week.tournament_id)
    if f is not None and len(f):
        tid, share = R.match_event(week.end_date, board["PLAYER"].tolist())
        if tid != week.tournament_id:
            raise ValueError(f"The golfodds board's names fit {tid} ({share:.0%}), not "
                             f"{week.tournament_id} {week.name}. Nothing saved.")
    else:
        # No entry list published yet: fall back to the board's own header date.
        scraped_end = board.attrs.get("scraped_end_date")
        if scraped_end != week.end_date:
            raise ValueError(f"The golfodds board ends {scraped_end}, this week ends "
                             f"{week.end_date}, and there is no entry list yet to check "
                             "its names against. Nothing saved.")
        print("  (no entry list published yet: checked by the board's date, not its names)")
        tid = week.tournament_id
    board = board.assign(ENDING_DATE=str(week.end_date),
                         SCRAPED_AT=datetime.now(timezone.utc).isoformat(timespec="seconds"))
    if save:
        _save_board(board, week, "golfodds")
    out = [R.resolve(n, tid) for n in board["PLAYER"]]
    board["player_id"], board["how"] = [o[0] for o in out], [o[1] for o in out]
    _report_unresolved(board.rename(columns={"PLAYER": "name"}), "golfodds")
    return board


# ---------------------------------------------------------------- 7-8. publish, open

def _slate_config() -> dict:
    import json
    m = json.loads((DATA / "api_week.json").read_text(encoding="utf-8"))
    return {"new": {"name": m["name"], "course": m["course"], "season": int(m["season"]),
                    "ending_date": pd.Timestamp(m["ending_date"])}}


def publish(export_df: pd.DataFrame | None = None) -> None:
    """Rebuild data/dashboard.db and write this notebook's slate for the
    dashboard. Reads the saved export and week marker, so it also runs on its
    own (the other computer, after a pull)."""
    from pga_api import dashboard_db, model
    from utils.dashboard import export_dashboard
    if export_df is None:
        if not model.EXPORT_CSV.exists():
            raise FileNotFoundError("No scored field saved yet: run sections 7 and 8 once "
                                    "(or git pull one the other computer saved).")
        export_df = pd.read_csv(model.EXPORT_CSV, dtype={"player_id": str})
    dashboard_db.write()
    export_dashboard(str(dashboard_db.PATH), export_df[[c for c in model.EXPORT_COLS if c in export_df]],
                     _slate_config(), db_url="data/dashboard.db", source="API data")


def open_dashboard(lineup_dir: str | None = None) -> None:
    """Show this notebook's slate. slate.js is shared with pga-dk.ipynb, so it is
    rewritten from this notebook's saved export every time: whichever notebook
    opened the dashboard last is the one it shows, and the top bar says which."""
    from utils.dashboard import resolve_lineup_dir, serve_dashboard
    publish()
    print("lineups ->", resolve_lineup_dir(lineup_dir))
    serve_dashboard(lineup_dir=lineup_dir)


# ---------------------------------------------------------------- names

def _report_unresolved(df: pd.DataFrame, source: str) -> None:
    miss = df[df["player_id"].isna()]
    if miss.empty:
        print(f"  every {source} name resolved to a Tour player id.")
        return
    print(f"  {len(miss)} {source} name(s) not resolved. For each golfer, find his id "
          f"with find_player('surname') and add:")
    for n in miss["name"]:
        print(f"      add_alias({n!r}, '<player_id>')")


def find_player(text: str) -> pd.DataFrame:
    """Golfers whose name contains `text`, with their most recent start."""
    pl, res, ev = _read("players"), _read("results"), _read("events")
    hit = pl[pl["name"].str.contains(text, case=False, na=False)]
    last = (res.merge(ev[["tournament_id", "name", "end_date"]].rename(columns={"name": "event"}))
            .sort_values("end_date").groupby("player_id").tail(1))
    return hit.merge(last[["player_id", "event", "end_date"]], on="player_id", how="left")


def add_alias(name: str, player_id: str, evidence: str = "added in the weekly notebook") -> None:
    a = pd.read_csv(ALIASES, dtype=str) if ALIASES.exists() else pd.DataFrame(
        columns=["name", "player_id", "evidence"])
    a = a[a["name"] != name]
    a = pd.concat([a, pd.DataFrame([{"name": name, "player_id": str(player_id),
                                     "evidence": evidence}])])
    a.sort_values("name").to_csv(ALIASES, index=False)
    print(f"alias saved: {name!r} -> {player_id}  (data/player_aliases.csv; commit it)")
