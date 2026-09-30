"""Name spelling: accents off, and the hand-made maps in data/name_mappings.json.

The Tour's player ids are what everything joins on; names only matter where
they come in from outside (odds boards, DraftKings files, the logs imported
from golf.db). identity.Resolver turns those into ids within each event's
field. The maps are older than the resolver: golf.db's spellings for names no
rule bridges, still applied to the DraftKings files and golfodds boards they
were written for. New fixes go in data/player_aliases.csv (weekly.add_alias).
"""

from __future__ import annotations

import json
import unicodedata
from pathlib import Path

import pandas as pd

MAPPINGS_PATH = Path(__file__).resolve().parent.parent / "data" / "name_mappings.json"


def _load() -> dict:
    try:
        return json.loads(MAPPINGS_PATH.read_text(encoding="utf-8"))
    except (FileNotFoundError, ValueError):
        return {}


_maps = _load()
DK_PLAYER_NAME_MAP = dict(_maps.get("DK_PLAYER_NAME_MAP", {}))      # DraftKings file -> golf.db
PLAYER_NAME_MAP = dict(_maps.get("PLAYER_NAME_MAP", {}))            # odds / results -> golf.db
TOURNAMENT_NAME_MAP = dict(_maps.get("TOURNAMENT_NAME_MAP", {}))


def normalize_name(name: str) -> str:
    if not isinstance(name, str):
        return name
    # NFKD strips combining accents (é->e) but silently DELETES characters with
    # no decomposition (ø, æ, đ, ß, ł) — that's how Thorbjørn became "Thorbjrn".
    # Transliterate those explicitly first.
    for a, b in (("ø", "o"), ("Ø", "O"), ("æ", "ae"), ("Æ", "Ae"),
                 ("ð", "d"), ("Ð", "D"), ("đ", "d"), ("Đ", "D"),
                 ("ß", "ss"), ("ł", "l"), ("Ł", "L"), ("þ", "th"), ("Þ", "Th")):
        name = name.replace(a, b)
    return unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("utf-8").strip()


def standardize_player_names(df: pd.DataFrame, player_column: str = "PLAYER") -> pd.DataFrame:
    """Accents off and PLAYER_NAME_MAP applied, in place on `player_column`."""
    if player_column not in df.columns:
        raise ValueError(f"'{player_column}' column not found in DataFrame.")
    df[player_column] = df[player_column].astype(str).map(normalize_name).replace(PLAYER_NAME_MAP)
    return df
