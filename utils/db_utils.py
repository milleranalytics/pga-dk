# db_utils.py


# == DRAFTKINGS NAME MAP ==
# Updates DraftKings player names to match PGA naming conventions
DK_PLAYER_NAME_MAP = {
    'Kyoung-Hoon Lee'     : 'K.H. Lee',
    'Erik Van Rooyen'     : 'Erik van Rooyen',
    'Cameron Davis'       : 'Cam Davis',
    'Dawie Van der Walt'  : 'Dawie van der Walt',
    'Hao-Tong Li'         : 'Haotong Li',
    'Vincent Whaley'      : 'Vince Whaley',
    'Sebastian Munoz'     : 'Sebastián Muñoz',
    'Sang-Moon Bae'       : 'Sangmoon Bae',
    'Fabian Gomez'        : 'Fabián Gómez',
    'Nicolas Echavarria'  : 'Nico Echavarria',
    'Zachary Bauchou'     : 'Zach Bauchou',
    'Adrien Dumont De Chassart': 'Adrien Dumont de Chassart',
    'Seung-Yul Noh'       : 'S.Y. Noh'
}

# == TOURNAMNET DICTIONARY ==
# Tournament names should match the PGA Tour website & stats/tournament names in database
TOURNAMENT_NAME_MAP = {
    'Sanderson Farms Champ'     : 'Sanderson Farms Championship',
    'Shriners H for C Open'     : 'Shriners Children\'s Open', # Note: I might have messed this up, not realizing older tournaments were Shriners Hospitals for Children Open and now don't match.
    'Sentry Tourn of Champions' : 'Sentry Tournament of Champions',
    'Pebble Beach Pro-Am'       : 'AT&T Pebble Beach Pro-Am',
    'AT&T Pebble Beach P-A'     : 'AT&T Pebble Beach Pro-Am',
    'Phoenix Open'              : 'Waste Management Phoenix Open',
    'Waste Mgt Phoenix Open'    : 'Waste Management Phoenix Open',
    'W M Phoenix Open'          : 'Waste Management Phoenix Open',
    'Arnold Palmer Invitational': 'Arnold Palmer Invitational presented by Mastercard',
    'THE PLAYERS Champ'         : 'THE PLAYERS Championship',
    'The Players Championship'  : 'THE PLAYERS Championship',
    'The Masters'               : 'Masters Tournament',
    'DEAN & DELUCA Invit'       : 'DEAN & DELUCA Invitational',
    'Dean & DeLuca Invit'       : 'DEAN & DELUCA Invitational',
    'US Open'                   : 'U.S. Open',
    'British Open'              : 'The Open Championship',
    'Open Championship'         : 'The Open Championship',
    'The ZOZO Championship'     : 'ZOZO CHAMPIONSHIP',
    'ZOZO Championship'         : 'ZOZO CHAMPIONSHIP',
    'RSM Classic'               : 'The RSM Classic',
    'Sentry TOC'                : 'Sentry Tournament of Champions',
    'SBS Tourn of Champions'    : 'SBS Tournament of Champions',
    'Hyundai Tourn of Champ'    : 'Hyundai Tournament of Champions',
    'The Masters'               : 'Masters Tournament',
    'Wells Fargo Champ'         : 'Wells Fargo Championship',
    'WGC-FedEx St. Jude Invit'  : 'World Golf Championships-FedEx St. Jude Invitational',
    'Cognizant Classic'         : 'Cognizant Classic in The Palm Beaches',
    "TX Children's Houston Open": 'Texas Children\'s Houston Open',
    "World Wide Tech Champ"     : 'World Wide Technology Championship'
    # Memorial Tournament not a clear mapping due to it having different sponsors yet always with Memorial.  Do that manually in DB Browser.
    # Bermuda Championship to Butterfield Bermuda Championship not obvious how to keep the old while swapping to the new.  Just wing it this week.
}

# == PLAYER DICTIONARY ==
# Player names should match the PGA Tour website & stats/tournament names in database
PLAYER_NAME_MAP = {
    'Rafael Cabrera Bello'    : 'Rafa Cabrera Bello',
    'Kyung-Tae Kim'           : 'K.T. Kim',
    'Byeong-Hun An'           : 'Byeong Hun An',
    'Cheng-Tsung Pan'         : 'C.T. Pan',
    'Sang-Moon Bae'           : 'Sangmoon Bae',
    'Sebastian Munoz'         : 'Sebastián Muñoz',
    'Ludvig Åberg'            : 'Ludvig Aberg',
    'Seung-Yul Noh'           : 'S.Y. Noh'
}


# == NAME MAP OVERRIDES (added from the notebook, persisted to JSON) ==
import json as _json
import os as _os
_OVERRIDES_PATH = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))),
                                "data", "name_overrides.json")

def _load_name_overrides() -> dict:
    try:
        with open(_OVERRIDES_PATH, encoding="utf-8") as f:
            return _json.load(f)
    except (FileNotFoundError, ValueError):
        return {}

_ov = _load_name_overrides()
DK_PLAYER_NAME_MAP.update(_ov.get("DK_PLAYER_NAME_MAP", {}))
PLAYER_NAME_MAP.update(_ov.get("PLAYER_NAME_MAP", {}))

def add_name_mapping(wrong: str, correct: str, scope: str = "player"):
    """Persist a name mapping without editing db_utils.py.

    scope="player": odds/results-side names (PLAYER_NAME_MAP)
    scope="dk":     DraftKings salary-file names (DK_PLAYER_NAME_MAP)
    Takes effect immediately and on every future run."""
    if scope not in ("player", "dk"):
        raise ValueError("scope must be 'player' or 'dk'")
    key = "DK_PLAYER_NAME_MAP" if scope == "dk" else "PLAYER_NAME_MAP"
    ov = _load_name_overrides()
    ov.setdefault(key, {})[wrong] = correct
    with open(_OVERRIDES_PATH, "w", encoding="utf-8") as f:
        _json.dump(ov, f, indent=2, ensure_ascii=False)
    (DK_PLAYER_NAME_MAP if scope == "dk" else PLAYER_NAME_MAP)[wrong] = correct
    print(f"✅ {key}: '{wrong}' -> '{correct}' (saved to data/name_overrides.json)")

def show_name_overrides():
    ov = _load_name_overrides()
    if not any(ov.values() if ov else []):
        print("No overrides yet.")
    for key, mappings in ov.items():
        print(f"{key}:")
        for w, c in mappings.items():
            print(f"  '{w}' -> '{c}'")


# Rename players based on PLAYER NAME MAP
import unicodedata
import pandas as pd
from sqlalchemy import create_engine

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

def clean_player_names_in_table(db_path: str, table_name: str, player_map: dict) -> pd.DataFrame:
    """
    Cleans and normalizes PLAYER names in a given SQLite table using a mapping and Unicode normalization.
    Returns a DataFrame of updated rows.
    """
    engine = create_engine(f"sqlite:///{db_path}")

    with engine.begin() as conn:
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)

        if "PLAYER" not in df.columns:
            print(f"⚠️ Table '{table_name}' has no PLAYER column.")
            return pd.DataFrame()

        df["PLAYER_ORIG"] = df["PLAYER"]
        df["PLAYER"] = df["PLAYER"].astype(str).map(normalize_name)
        df["PLAYER"] = df["PLAYER"].replace(player_map)

        updated = df[df["PLAYER"] != df["PLAYER_ORIG"]].copy()

        if updated.empty:
            print(f"ℹ️ No player names needed updates in '{table_name}'.")
        else:
            df = df.drop(columns=["PLAYER_ORIG"])
            print(f"✅ Updating {len(updated)} rows in '{table_name}' with normalized names.")
            df.to_sql(table_name, conn, index=False, if_exists="replace")

    engine.dispose()
    return updated


# Standardize player names
def standardize_player_names(df: pd.DataFrame, player_column: str = "PLAYER") -> pd.DataFrame:
    """
    Cleans and standardizes player names in-place on the specified column.
    Applies Unicode normalization and player name map.
    """
    if player_column not in df.columns:
        raise ValueError(f"'{player_column}' column not found in DataFrame.")

    df[player_column] = (
        df[player_column]
        .astype(str)
        .map(normalize_name)
        .replace(PLAYER_NAME_MAP)
    )
    return df

# region --- Update Tournament
# -----------------------------------------------------
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
import requests
import urllib3
from numpy import nan
from utils.schema import tournaments_table, metadata  # your SQLAlchemy table definition
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

X_API_KEY = "da2-gsrx5bibzbb4njvhl7t37wqyl4"

def update_tournament_results(config: dict, db_path: str, season: int, year: int, verify_ssl=False):
    """Scrapes and updates tournament results into the refactored SQLAlchemy-based tournaments table."""
    
    tourn_id = config["old"]["id"]
    tourn_name = config["old"]["name"]
    course = config["old"]["course"]
    date_str = config["old"]["date"]

    print(f"📦 Fetching results for tournament ID {tourn_id} ({tourn_name}), year: {year}")

    payload = {
        "operationName": "TournamentPastResults",
        "variables": {"tournamentPastResultsId": tourn_id, "year": year},
        "query": """query TournamentPastResults($tournamentPastResultsId: ID!, $year: Int) {
            tournamentPastResults(id: $tournamentPastResultsId, year: $year) {
                id
                players {
                    id
                    position
                    player {
                        displayName
                    }
                    rounds {
                        parRelativeScore
                    }
                    additionalData
                }
            }
        }"""
    }

    if not verify_ssl:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    try:
        response = requests.post(
            "https://orchestrator.pgatour.com/graphql",
            json=payload,
            headers={"x-api-key": X_API_KEY},
            verify=verify_ssl
        )
        response.raise_for_status()
    except Exception as e:
        print("❌ API request failed:", e)
        return None

    try:
        players = response.json()["data"]["tournamentPastResults"]["players"]
    except Exception as e:
        print("❌ Error parsing JSON response:", e)
        raise

    if not players:
        print("⚠️ No players found in response.")
        return None

    df = pd.DataFrame(map(lambda p: {
        "POS": p["position"],
        "PLAYER": p["player"]["displayName"],
        "ROUNDS:1": p["rounds"][0]["parRelativeScore"] if len(p["rounds"]) > 0 else nan,
        "ROUNDS:2": p["rounds"][1]["parRelativeScore"] if len(p["rounds"]) > 1 else nan,
        "ROUNDS:3": p["rounds"][2]["parRelativeScore"] if len(p["rounds"]) > 2 else nan,
        "ROUNDS:4": p["rounds"][3]["parRelativeScore"] if len(p["rounds"]) > 3 else nan,
        "OFFICIAL_MONEY": p["additionalData"][1],
        "FEDEX_CUP_POINTS": p["additionalData"][0],
    }, players))

    df = df.dropna(subset=["POS"])
    df["FINAL_POS"] = df["POS"].str.extract(r"(\d+)", expand=False).fillna(90).astype(int)
    df.insert(0, "SEASON", season)
    end_date = datetime.strptime(date_str, "%m/%d/%Y").date()
    # Tournaments end Sunday (Saturday for Farmers-style, Monday for weather
    # delays) — a Tue-Fri date is almost certainly the START date typed by
    # mistake, which silently breaks odds joins and rolling windows.
    if end_date.weekday() in (1, 2, 3, 4):  # Tue-Fri
        raise ValueError(
            f"ENDING_DATE {end_date} is a {end_date.strftime('%A')} — tournaments "
            f"end Sat/Sun/Mon. Check the 'tournament_date' in the User Input cell "
            f"(did you enter the start date?).")
    df.insert(1, "ENDING_DATE", end_date)
    df.insert(2, "TOURN_ID", tourn_id)
    df.insert(3, "TOURNAMENT", tourn_name)
    df.insert(4, "COURSE", course)

    # Clean up player names before inserting
    df = standardize_player_names(df)

    # Connect with SQLAlchemy
    engine = create_engine(f"sqlite:///{db_path}")

    with engine.begin() as conn:
        # Load existing rows by PK
        existing_keys = pd.read_sql(
            "SELECT ENDING_DATE, TOURNAMENT, PLAYER FROM tournaments", conn
        )

        # Normalize ENDING_DATE types to ensure merge matches correctly
        df["ENDING_DATE"] = pd.to_datetime(df["ENDING_DATE"]).dt.date
        existing_keys["ENDING_DATE"] = pd.to_datetime(existing_keys["ENDING_DATE"]).dt.date

        # Anti-join: only keep new rows not already in the table
        new_df = df.merge(
            existing_keys,
            on=["ENDING_DATE", "TOURNAMENT", "PLAYER"],
            how="left",
            indicator=True
        )
        new_df = new_df[new_df["_merge"] == "left_only"].drop(columns=["_merge"])

        if new_df.empty:
            print(f"ℹ️ Tournament '{tourn_name}' already exists — no new data inserted.")
        else:
            new_df.to_sql("tournaments", conn, index=False, if_exists="append")
            print(f"✅ {len(new_df)} new rows added for '{tourn_name}'")

    engine.dispose()
    return df



# endregion

# region --- Update Stats
from sqlalchemy import create_engine
from sqlalchemy import text
import pandas as pd
import requests
from numpy import nan
from utils.schema import stats_table, metadata

X_API_KEY = "da2-gsrx5bibzbb4njvhl7t37wqyl4"

def update_season_stats(stats_year: int, db_path: str, verify_ssl=False) -> pd.DataFrame:
    """Scrapes PGA stat categories for a given year and updates the stats table in the database."""

    stat_ids = {
        "SGTTG": "02674", "SGOTT": "02567", "SGAPR": "02568", "SGATG": "02569", "SGP": "02564",
        "BIRDIES": "352", "PAR_3": "142", "PAR_4": "143", "PAR_5": "144",
        "TOTAL_DRIVING": "129", "DRIVING_DISTANCE": "101", "DRIVING_ACCURACY": "102",
        "GIR": "103", "SCRAMBLING": "130", "OWGR": "186"
    }

    def get_stats(year: int, stat_id: str) -> pd.DataFrame:
        payload = {
            "operationName": "StatDetails",
            "variables": {
                "tourCode": "R",
                "statId": stat_id,
                "year": year,
                "eventQuery": None
            },
            "query": "query StatDetails($tourCode: TourCode!, $statId: String!, $year: Int, $eventQuery: StatDetailEventQuery) {\n  statDetails(\n    tourCode: $tourCode\n    statId: $statId\n    year: $year\n    eventQuery: $eventQuery\n  ) {\n    rows {\n      ... on StatDetailsPlayer {\n        playerName\n        rank\n        stats {\n          statValue\n        }\n      }\n    }\n  }\n}"
        }

        try:
            res = requests.post(
                "https://orchestrator.pgatour.com/graphql",
                json=payload,
                headers={"x-api-key": X_API_KEY},
                verify=verify_ssl
            )
            res.raise_for_status()
            json_data = res.json()
            rows = json_data.get("data", {}).get("statDetails", {}).get("rows", [])
        except Exception as e:
            print(f"❌ Failed to fetch or parse stat {stat_id}: {e}")
            return pd.DataFrame(columns=["PLAYER", "RANK", "VALUE"])

        players = filter(lambda r: r.get("playerName"), rows)
        table = []
        for r in players:
            try:
                player = r["playerName"]
                rank = r.get("rank")
                stats = r.get("stats", [])
                value = stats[0]["statValue"] if stats else nan
                table.append({"PLAYER": player, "RANK": rank, "VALUE": value})
            except Exception as e:
                print(f"⚠️ Skipping row due to parsing error: {e}")

        return pd.DataFrame(table)

    # Gather stats for all categories
    base_stat = "SGTTG"
    stat_frames = {}

    for stat_name, stat_id in stat_ids.items():
        df = get_stats(stats_year, stat_id)
        df = df.rename(columns={
            "RANK": f"{stat_name}_RANK",
            "VALUE": stat_name
        })
        stat_frames[stat_name] = df

    # Merge all stats into one dataframe
    stats_df = stat_frames[base_stat]
    for stat_name, df in stat_frames.items():
        if stat_name != base_stat:
            stats_df = stats_df.merge(df, on="PLAYER", how="outer")

    stats_df["SEASON"] = stats_year
    stats_df["PLAYER"] = stats_df["PLAYER"].astype(str).str.strip()
    stats_df["SEASON"] = stats_df["SEASON"].astype(int)

    # Fill in missing schema columns if needed
    required_columns = [col.name for col in stats_table.columns]
    for col in required_columns:
        if col not in stats_df.columns:
            stats_df[col] = None

    # Standardize names
    stats_df = standardize_player_names(stats_df)

    # Overwrite season's stats
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        # Delete any existing records for this season
        conn.execute(text(f"DELETE FROM stats WHERE SEASON = {stats_year}"))

        # Insert fresh data
        stats_df.to_sql("stats", conn, index=False, if_exists="append")
        print(f"✅ Overwrote stats for season {stats_year} with {len(stats_df)} rows.")

    engine.dispose()

    return stats_df


# endregion

# region --- Odds 

# == MANUAL CLEANUP HELPER to run if we have NaN values for Odds ==
from sqlalchemy import create_engine
import pandas as pd
from utils.schema import odds_table, metadata

def clean_odds_names(db_path: str, tournament_map: dict, player_map: dict) -> pd.DataFrame:
    """Cleans up mismatched player and tournament names in the odds table using provided mapping dictionaries."""

    engine = create_engine(f"sqlite:///{db_path}")

    with engine.begin() as conn:
        df = pd.read_sql("SELECT * FROM odds", conn)

        # Track original values for comparison
        df["TOURNAMENT_ORIG"] = df["TOURNAMENT"]
        df["PLAYER_ORIG"] = df["PLAYER"]

        # Apply name maps
        df["TOURNAMENT"] = df["TOURNAMENT"].replace(tournament_map)
        df["PLAYER"] = df["PLAYER"].replace(player_map)

        # Find rows that changed
        updated = df[
            (df["TOURNAMENT"] != df["TOURNAMENT_ORIG"]) |
            (df["PLAYER"] != df["PLAYER_ORIG"])
        ].copy()

        if updated.empty:
            print("ℹ️ No odds rows required name cleanup.")
        else:
            df = df.drop(columns=["TOURNAMENT_ORIG", "PLAYER_ORIG"])
            metadata.drop_all(conn, tables=[odds_table])
            metadata.create_all(conn)
            df.to_sql("odds", conn, index=False, if_exists="append")
            print(f"✅ Cleaned and updated {len(updated)} rows in 'odds' table.")

    engine.dispose()
    return updated



from sqlalchemy import create_engine
import pandas as pd
import requests
import numpy as np
from utils.schema import odds_table, metadata
from utils.db_utils import TOURNAMENT_NAME_MAP, PLAYER_NAME_MAP
from io import StringIO


def import_historical_odds(odds_year: str, season: int, db_path: str, url: str = None) -> pd.DataFrame:
    import pandas as pd
    import numpy as np
    import requests
    import re
    from io import StringIO
    from datetime import datetime
    from sqlalchemy import create_engine
    from utils.schema import odds_table, metadata
    from utils.db_utils import TOURNAMENT_NAME_MAP, PLAYER_NAME_MAP

    if url is None:
        url = f"http://golfodds.com/archives-{odds_year}.html"
    response = requests.get(url)
    tables = pd.read_html(StringIO(response.text))
    # Find the largest 2-column table that contains at least some odds-like strings
    raw_df = None
    for tbl in tables:
        if tbl.shape[1] == 2 and tbl.shape[0] > 50:  # Basic filter for size and structure
            sample = tbl.iloc[:, 1].astype(str).str.contains(r"\d+/\d+").sum()
            if sample > 5:  # Odds-like pattern detected
                raw_df = tbl
                break

    if raw_df is None:
        raise ValueError("❌ Could not find valid odds table on the page.")

    df = raw_df.dropna(how="all").reset_index(drop=True)
    df.columns = ["PLAYER", "ODDS"]

# 🔧 Clean up non-breaking spaces and normalize whitespace in PLAYER column
    df["PLAYER"] = (
        df["PLAYER"]
        .astype(str)
        .str.replace("\u00A0", " ", regex=False)  # non-breaking space
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )

    df.insert(0, "SEASON", season)
    df.insert(1, "TOURNAMENT", value=np.nan)
    df.insert(2, "ENDING_DATE", value=np.nan)

    def parse_ending_date(text):
        import re
        from datetime import datetime

        # Normalize whitespace and symbols
        text = (
            text.replace("\u2013", "-")
                .replace("–", "-")
                .replace("\xa0", " ")
        )
        text = re.sub(r"\bSept(?!ember)\b", "Sep", text)

        # ✅ Fix typo: "Match" → "March" only when it's part of a date range
        text = re.sub(r"\bMatch(?=\s+\d{1,2}\s*[-–]\s*\d{1,2},\s*\d{4})", "March", text)

        # Pattern 1: "July 30 - August 2, 2015" or "Oct 29 - Nov 1, 2015"
        match = re.search(r"(\w+)\s\d+\s*-\s*(\w+)\s(\d+),\s(\d{4})", text)
        if match:
            month2, day2, year = match.group(2), match.group(3), match.group(4)
            for fmt in ["%B %d, %Y", "%b %d, %Y"]:
                try:
                    return datetime.strptime(f"{month2} {day2}, {year}", fmt).date()
                except ValueError:
                    continue

        # Pattern 2: "November 21-24, 2024"
        match = re.search(r"(\w+)\s\d+-\d+,\s(\d{4})", text)
        if match:
            month, year = match.group(1), match.group(2)
            day = re.search(r"(\d+)-(\d+)", text).group(2)
            for fmt in ["%B %d, %Y", "%b %d, %Y"]:
                try:
                    return datetime.strptime(f"{month} {day}, {year}", fmt).date()
                except ValueError:
                    continue

        # Pattern 3: "Sunday, October 20, 2019"
        try:
            return datetime.strptime(text.strip(), "%A, %B %d, %Y").date()
        except ValueError:
            pass

        # Pattern 4: "October 20, 2019"
        try:
            return datetime.strptime(text.strip(), "%B %d, %Y").date()
        except ValueError:
            pass

        return None
    

    # === STEP 3: Iterate block by block ===
    final_rows = []
    i = 0
    last_tourn_name = None
    last_end_date = None

    while i < len(df) - 4:
        player_i = str(df.loc[i, "PLAYER"])
        player_i2 = str(df.loc[i + 2, "PLAYER"])
        player_i3 = str(df.loc[i + 3, "PLAYER"]).lower()

        is_header = (
            pd.isna(df.loc[i, "ODDS"]) and
            pd.isna(df.loc[i + 1, "ODDS"]) and (
                re.search(r"\w+\s\d+\s*[-–]\s*(\w+\s)?\d+,\s\d{4}", player_i2) or
                re.search(r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+\w+\s\d{1,2},\s\d{4}", player_i2)
            )
        )

        if is_header:
            tourn_name = player_i.strip()
            end_date = parse_ending_date(player_i2)

            if "cancelled" in player_i3:
                i += 4
                continue

            if tourn_name == last_tourn_name and end_date == last_end_date:
                i += 1
                continue

            last_tourn_name = tourn_name
            last_end_date = end_date
            i += 4

            while i < len(df) - 2:
                next_i2 = str(df.loc[i + 2, "PLAYER"])
                is_next_header = (
                    pd.isna(df.loc[i, "ODDS"]) and
                    pd.isna(df.loc[i + 1, "ODDS"]) and (
                        re.search(r"\w+\s\d+\s*[-–]\s*(\w+\s)?\d+,\s\d{4}", next_i2) or
                        re.search(r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+\w+\s\d{1,2},\s\d{4}", next_i2)
                    )
                )
                if is_next_header:
                    break

                if pd.notna(df.loc[i, "ODDS"]):
                    row = df.loc[i].copy()
                    row["TOURNAMENT"] = tourn_name
                    row["ENDING_DATE"] = end_date
                    final_rows.append(row)
                i += 1
        else:
            i += 1

    clean_df = pd.DataFrame(final_rows)
    clean_df["PLAYER"] = clean_df["PLAYER"].str.replace(r"\s\*Winner\*", "", regex=True)

    clean_df["VEGAS_ODDS"] = (
        clean_df["ODDS"]
        .str.replace(",", "")
        .str.extract(r"(\d+)/(\d+)")
        .astype(float)
        .apply(lambda x: x[0] / x[1], axis=1)
    )

    # Normalize and apply name maps
    clean_df["PLAYER"] = clean_df["PLAYER"].str.replace(r"\s+", " ", regex=True).str.strip()
    clean_df["TOURNAMENT"] = clean_df["TOURNAMENT"].str.replace(r"\s+", " ", regex=True).str.strip()
    clean_df["TOURNAMENT"] = clean_df["TOURNAMENT"].replace(TOURNAMENT_NAME_MAP)
    clean_df["PLAYER"] = clean_df["PLAYER"].replace(PLAYER_NAME_MAP)
    clean_df = standardize_player_names(clean_df)

    final_df = clean_df[["SEASON", "TOURNAMENT", "ENDING_DATE", "PLAYER", "ODDS", "VEGAS_ODDS"]].copy()
    # Archive pages sometimes list the same player twice within a tournament block
    # (e.g., updated odds lines); keep the first listing so the PK insert can't fail.
    final_df = final_df.drop_duplicates(subset=["SEASON", "TOURNAMENT", "ENDING_DATE", "PLAYER"], keep="first")
    # 🚫 Remove team events that don't apply to fantasy scoring
    final_df = final_df[~final_df["TOURNAMENT"].str.contains("Presidents Cup|Ryder Cup", case=False, na=False)]


    # === Write to DB ===
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        existing = pd.read_sql("SELECT SEASON, TOURNAMENT, ENDING_DATE, PLAYER FROM odds", conn)

        # Normalize merge keys
        for col in ["TOURNAMENT", "PLAYER"]:
            existing[col] = existing[col].astype(str).str.strip()
            final_df[col] = final_df[col].astype(str).str.strip()

        existing["ENDING_DATE"] = pd.to_datetime(existing["ENDING_DATE"]).dt.date
        final_df["ENDING_DATE"] = pd.to_datetime(final_df["ENDING_DATE"]).dt.date

        deduped_df = final_df.merge(
            existing,
            on=["SEASON", "TOURNAMENT", "ENDING_DATE", "PLAYER"],
            how="left",
            indicator=True
        )
        deduped_df = deduped_df[deduped_df["_merge"] == "left_only"].drop(columns="_merge")

        if deduped_df.empty:
            print(f"ℹ️ Historical odds for season {season} already exist — no new rows added.")
        else:
            print(f"✅ Inserting {len(deduped_df)} new rows into odds table...")
            deduped_df.to_sql("odds", conn, index=False, if_exists="append")

    engine.dispose()
    return final_df




# endregion



# region --- Odds Current
# -----------------------------------------------------

import pandas as pd
import requests
import io

def get_current_week_odds(season: int, tournament_name: str, url: str = "http://golfodds.com/weekly-odds.html") -> pd.DataFrame:
    """
    Scrapes and cleans current week odds from GolfOdds.com.
    Returns a DataFrame with SEASON, TOURNAMENT, PLAYER, ODDS (string), and VEGAS_ODDS (decimal).
    """
    headers = {'User-Agent': 'Mozilla/5.0'}
    html = requests.get(url, headers=headers).text

    # Parse the page's own tournament header so callers can verify the board
    # matches their config (guards against a stale User Input cell).
    import re as _re
    from datetime import datetime as _dt
    scraped_name, scraped_end = None, None
    m = _re.search(r'class="Headline-orange">([^<]+)</span>', html)
    if m:
        scraped_name = m.group(1).strip()
    m2 = _re.search(
        r'(January|February|March|April|May|June|July|August|September|October|November|December)'
        r'\s+\d{1,2}\s*-\s*'
        r'(?:(January|February|March|April|May|June|July|August|September|October|November|December)\s+)?'
        r'(\d{1,2}),\s*(\d{4})', html)
    if m2:
        end_month = m2.group(2) or m2.group(1)
        try:
            scraped_end = _dt.strptime(f"{end_month} {m2.group(3)}, {m2.group(4)}", "%B %d, %Y").date()
        except ValueError:
            pass

    odds_df = pd.read_html(io.StringIO(html))[3]

    # Drop all-NaN rows and reset index
    odds_df = odds_df.dropna(how='all').reset_index(drop=True)

    # Rename first two columns
    odds_df = odds_df.rename(columns={0: "PLAYER", 1: "ODDS"})

    # Insert season and tournament info
    odds_df.insert(loc=0, column="SEASON", value=season)
    odds_df.insert(loc=1, column="TOURNAMENT", value=tournament_name)

    # Drop rows with missing player names
    odds_df = odds_df.dropna(subset=["PLAYER"])

    # Trim rows after "Tournament Matchups" section
    try:
        matchups_row = odds_df.index[odds_df.iloc[:, 2].astype(str).str.contains("Tournament")].tolist()[0]
        odds_df = odds_df.iloc[:matchups_row]
    except IndexError:
        pass  # If not found, continue without trimming

    # Remove entries that are not valid odds
    odds_df = odds_df[~odds_df["ODDS"].isin(["WD", "XX", "ODDS to Win:", "ODDS to\xa0Win:"])]

    # Clean formatting
    odds_df["ODDS"] = odds_df["ODDS"].str.replace(",", "", regex=True)
    odds_df["PLAYER"] = odds_df["PLAYER"].str.replace(r"\s", " ", regex=True)

    # Convert fractional odds to decimal
    try:
        odds_df["VEGAS_ODDS"] = (
            odds_df["ODDS"].str.split("/").str[0].astype(float) /
            odds_df["ODDS"].str.split("/").str[1].astype(float)
        )
    except Exception:
        odds_df["VEGAS_ODDS"] = None  # fallback if conversion fails

    # Apply name normalization maps
    odds_df["PLAYER"] = odds_df["PLAYER"].replace(PLAYER_NAME_MAP)
    odds_df["TOURNAMENT"] = odds_df["TOURNAMENT"].replace(TOURNAMENT_NAME_MAP)

    # Final column selection
    odds_df = odds_df[["SEASON", "TOURNAMENT", "PLAYER", "ODDS", "VEGAS_ODDS"]]

    
    # ✅ Normalize player names
    odds_df = standardize_player_names(odds_df)

    odds_df.attrs["scraped_tournament"] = scraped_name
    odds_df.attrs["scraped_end_date"] = scraped_end
    if scraped_name:
        print(f"ℹ️ Odds page is serving: {scraped_name}"
              + (f" (ends {scraped_end})" if scraped_end else ""))
    return odds_df

# endregion
