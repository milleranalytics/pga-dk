"""Names -> the Tour's player_id, and outside events -> its tournament_id.

A name is resolved WITHIN the field of the event it came from, which is what
makes short keys safe: "Tom Kim" and "Joohyung Kim" are one golfer at the event
where both spellings appear. In order:

  1. data/player_aliases.csv: names no rule can bridge (Joohyung Kim = Tom Kim),
     each with the evidence it was added on
  2. exact name key inside the field
  3. same surname and compatible first name inside the field (Matt/Matthew)
  4. exact name key anywhere, if exactly one golfer has it (pre-event withdrawals)

Anything else stays unresolved and is listed, never guessed.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from utils.db_utils import normalize_name

ALIASES = Path(__file__).resolve().parent.parent / "data" / "player_aliases.csv"
_SUFFIXES = {"jr", "sr", "ii", "iii", "iv"}


def _tokens(name: str) -> list[str]:
    n = normalize_name(str(name)).lower()
    n = re.sub(r"\(.*?\)", " ", n)            # "Zach Johnson (am)"
    n = re.sub(r"[.,'’`\-]", " ", n)
    return [t for t in n.split() if t not in _SUFFIXES]


def name_key(name: str) -> str:
    """'K.H. Lee' and 'KH Lee' -> 'khlee'; 'Frank Bensel, Jr.' -> 'frankbensel'."""
    return "".join(_tokens(name))


def _initial_last(name: str) -> str:
    t = _tokens(name)
    return f"{t[0][0]}|{t[-1]}" if len(t) >= 2 else ""


_NICKNAMES = {frozenset(p) for p in [
    ("mike", "michael"), ("mickey", "michael"), ("joe", "joseph"), ("tommy", "thomas"),
    ("tom", "thomas"), ("bill", "william"), ("will", "william"), ("billy", "william"),
    ("bob", "robert"), ("rob", "robert"), ("jim", "james"), ("jimmy", "james"),
    ("rick", "richard"), ("dick", "richard"), ("tony", "anthony"), ("andy", "andrew"),
    ("drew", "andrew"), ("zach", "zachary"), ("zac", "zachary"), ("jack", "john"),
    ("johnny", "john"), ("jon", "jonathan"), ("ollie", "oliver"), ("charlie", "charles"),
    ("harry", "henry"), ("fred", "frederick"), ("ted", "edward"), ("ed", "edward"),
]}


def same_golfer_name(a: str, b: str) -> bool:
    """Same surname and compatible first names: 'Matt NeSmith' / 'Matthew NeSmith',
    'K.H. Lee' / 'Kyoung-Hoon Lee'. Never on an initial alone: 'Danny Lee' and
    'D.H. Lee' are two golfers, and so are 'S.H. Kim' and 'Si Woo Kim'."""
    ta, tb = _tokens(a), _tokens(b)
    if len(ta) < 2 or len(tb) < 2 or ta[-1] != tb[-1]:
        return False
    fa, fb = ta[:-1], tb[:-1]
    ia, ib = "".join(t[0] for t in fa), "".join(t[0] for t in fb)
    if all(len(t) == 1 for t in fa) or all(len(t) == 1 for t in fb):
        return ia == ib and len(ia) >= 2                 # K.H. == Kyoung-Hoon
    x, y = fa[0], fb[0]
    return (x == y or (min(len(x), len(y)) >= 3 and (x.startswith(y) or y.startswith(x)))
            or frozenset((x, y)) in _NICKNAMES)


def load_aliases() -> dict[str, str]:
    if not ALIASES.exists():
        return {}
    a = pd.read_csv(ALIASES, dtype=str).fillna("")
    return dict(zip(a["name"].map(name_key), a["player_id"]))   # "Chun An Yu" = "Chun-an Yu"


class Resolver:
    """Resolves names against pga.db's events, results and players frames."""

    def __init__(self, events: pd.DataFrame, results: pd.DataFrame, players: pd.DataFrame,
                 fields: dict[str, pd.DataFrame] | None = None):
        self.events = events.assign(end=pd.to_datetime(events["end_date"]))
        self.players = players.set_index("player_id")["name"]
        starts = results["player_id"].value_counts()
        # A shared name inside one field (two Zach Johnsons at the 2018 PGA) goes
        # to the golfer with more career starts: the one bookmakers price.
        self.starts = starts
        self._fields: dict[str, pd.DataFrame] = {}
        for tid, g in results.groupby("tournament_id"):
            self._fields[tid] = self._index(g["player_id"])
        for tid, f in (fields or {}).items():
            self._fields[tid] = self._index(f["player_id"])
        allp = self._index(pd.Series(self.players.index))
        self._global = allp.groupby("key")["player_id"].agg(list).to_dict()
        self.aliases = load_aliases()

    def _index(self, ids: pd.Series) -> pd.DataFrame:
        ids = ids.drop_duplicates()
        names = self.players.reindex(ids.values).fillna("")
        return pd.DataFrame({"player_id": ids.values, "name": names.values,
                             "key": names.map(name_key).values,
                             "il": names.map(_initial_last).values})

    def field(self, tournament_id: str) -> pd.DataFrame | None:
        return self._fields.get(tournament_id)

    def _pick(self, ids: list[str]) -> str:
        return max(ids, key=lambda i: self.starts.get(i, 0))

    def resolve(self, name: str, tournament_id: str | None = None) -> tuple[str | None, str]:
        """-> (player_id or None, how it was found)."""
        if name_key(name) in self.aliases:
            return self.aliases[name_key(name)], "alias"
        k, il = name_key(name), _initial_last(name)
        f = self.field(tournament_id) if tournament_id else None
        if f is not None:
            hit = f.loc[f["key"] == k, "player_id"].tolist()
            if hit:
                return self._pick(hit), "field" if len(hit) == 1 else "field-shared-name"
            hit = f.loc[f["name"].map(lambda n: same_golfer_name(n, name)), "player_id"].tolist()
            if len(hit) == 1:
                return hit[0], "field-nickname"
        hit = self._global.get(k, [])
        if len(hit) == 1:
            return hit[0], "global"
        return None, "unresolved"

    def match_event(self, end_date, names: list[str], near_days: int = 3,
                    min_share: float = 0.5) -> tuple[str | None, float]:
        """The Tour event ending within `near_days` of `end_date` whose field holds
        the largest share of `names`. The same week can carry the main event, an
        opposite-field event and a European Tour event, so the date alone is not
        enough; the names decide."""
        end = pd.to_datetime(end_date)
        cands = self.events[(self.events["end"] - end).abs() <= pd.Timedelta(days=near_days)]
        best, best_share = None, 0.0
        keys = [(name_key(n), _initial_last(n)) for n in names]
        for tid in cands["tournament_id"]:
            f = self.field(tid)
            if f is None or f.empty:
                continue
            fk, fil = set(f["key"]), set(f["il"])
            share = sum(k in fk or il in fil for k, il in keys) / max(len(keys), 1)
            if share > best_share:
                best, best_share = tid, share
        return (best, best_share) if best_share >= min_share else (None, best_share)
