"""Name spelling: accents off.

The Tour's player ids are what everything joins on; names only matter where
they come in from outside (odds boards, DraftKings files, the logs imported
from golf.db). identity.Resolver turns those into ids within each event's
field; a name no rule bridges gets an alias in data/player_aliases.csv
(weekly.add_alias). golf.db's hand-made name maps (name_mappings.json) were
retired to old_workflow/: the resolver matched 27 of their 30 names on its
own, and the other three became aliases.
"""

from __future__ import annotations

import unicodedata


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
