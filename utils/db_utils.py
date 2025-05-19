# db_utils.py


# region --- Update Tournament
# -----------------------------------------------------
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
import requests
import urllib3
from numpy import nan
from utils.schema import tournaments_table, metadata  # your SQLAlchemy table definition

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
    df.insert(1, "ENDING_DATE", datetime.strptime(date_str, "%m/%d/%Y").date())
    df.insert(2, "TOURN_ID", tourn_id)
    df.insert(3, "TOURNAMENT", tourn_name)
    df.insert(4, "COURSE", course)

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
import pandas as pd
import requests
from numpy import nan
from utils.schema import stats_table, metadata

X_API_KEY = "da2-gsrx5bibzbb4njvhl7t37wqyl4"

def update_season_stats(stats_year: int, db_path: str, verify_ssl=False) -> pd.DataFrame:
    """Scrapes PGA stat categories for a given year and updates the stats table in the database."""

    stat_ids = {
        "SG:TTG": "02674", "SG:OTT": "02567", "SG:APR": "02568", "SG:ATG": "02569", "SG:P": "02564",
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
    base_stat = "SG:TTG"
    stat_frames = {}

    for stat_name, stat_id in stat_ids.items():
        df = get_stats(stats_year, stat_id)
        df = df.rename(columns={
            "RANK": f"{stat_name.replace(':', '')}_RANK",
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

    # Deduplicate with SQLAlchemy
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        existing = pd.read_sql("SELECT SEASON, PLAYER FROM stats", conn)
        existing["PLAYER"] = existing["PLAYER"].astype(str).str.strip()

        new_rows = stats_df.merge(
            existing,
            on=["SEASON", "PLAYER"],
            how="left",
            indicator=True
        )
        new_rows = new_rows[new_rows["_merge"] == "left_only"].drop(columns=["_merge"])

        if new_rows.empty:
            print(f"ℹ️ Stats for season {stats_year} already exist — no new rows added.")
        else:
            new_rows.to_sql("stats", conn, index=False, if_exists="append")
            print(f"✅ {len(new_rows)} player stats inserted for season {stats_year}.")

    engine.dispose()

    return new_rows


# endregion

# region --- Odds 

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
    "TX Children's Houston Open": 'Texas Children\'s Houston Open'
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
    'Sebastian Munoz'         : 'Sebastián Muñoz'
}

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


# # == Update an entire year of historical odds (this is prior to ENDING DATE) ==
# def import_historical_odds(odds_year: str, season: int, db_path: str) -> pd.DataFrame:
#     """Imports and cleans historical Vegas odds for a given PGA season."""
    
#     url = f"http://golfodds.com/archives-{odds_year}.html"
#     html = StringIO(requests.get(url).text)
#     try:
#         raw_df = pd.read_html(html)[4]
#     except Exception as e:
#         print(f"❌ Failed to fetch or parse odds for {odds_year}: {e}")
#         return pd.DataFrame()

#     df = raw_df.dropna(how='all')
#     df = df.rename(columns={0: 'PLAYER', 1: 'ODDS'})
#     df.insert(loc=0, column='SEASON', value=season)
#     df.insert(loc=1, column='TOURNAMENT', value=np.nan)
#     df['PLAYER'] = df['PLAYER'].str.replace(r'\s\*Winner\*', '', regex=True)

#     # Identify tournament name rows and fill
#     is_tourn = (
#         df.ODDS.isna() &
#         df.ODDS.shift(-1).isna() &
#         df.ODDS.shift(-2).isna() &
#         df.ODDS.shift(-3).isna() &
#         df.ODDS.shift(-4).notna()
#     )
#     df["TOURNAMENT"] = df["TOURNAMENT"].mask(is_tourn, df["PLAYER"]).ffill()
#     df = df.dropna(subset=["ODDS"])

#     # Clean and convert odds to float
#     df["VEGAS_ODDS"] = df["ODDS"]
#     df["VEGAS_ODDS"] = (
#         df["VEGAS_ODDS"]
#         .str.replace(",", "", regex=True)
#         .str.replace("-", "", regex=True)
#         .str.replace(r"\s", "", regex=True)
#     )

#     # Filter out unwanted labels
#     drop_labels = ["DNS", "WD", "EVEN", "XX", "DNQ", "ODDS to Win:"]
#     df = df[~df["ODDS"].isin(drop_labels)]

#     # Fractional odds conversion
#     try:
#         df["VEGAS_ODDS"] = (
#             df["VEGAS_ODDS"].str.split("/").str[0].astype(float) /
#             df["VEGAS_ODDS"].str.split("/").str[1].astype(float)
#         )
#     except Exception as e:
#         print(f"⚠️ Odds conversion failed for some rows: {e}")
#         df = df[df["VEGAS_ODDS"].str.contains("/")]

    # # Normalize spacing and apply name maps
    # df["PLAYER"] = df["PLAYER"].str.replace(r"\s", " ", regex=True).str.strip()
    # df["TOURNAMENT"] = df["TOURNAMENT"].str.replace(r"\s", " ", regex=True).str.strip()
    # df["TOURNAMENT"] = df["TOURNAMENT"].replace(TOURNAMENT_NAME_MAP)
    # df["PLAYER"] = df["PLAYER"].replace(PLAYER_NAME_MAP)

#     # Insert into DB using SQLAlchemy
#     engine = create_engine(f"sqlite:///{db_path}")
#     with engine.begin() as conn:
#         existing = pd.read_sql("SELECT SEASON, TOURNAMENT, PLAYER, ODDS FROM odds", conn)
#         df["ODDS"] = df["ODDS"].astype(str).str.strip()
#         existing["ODDS"] = existing["ODDS"].astype(str).str.strip()

#         new_rows = df.merge(
#             existing,
#             on=["SEASON", "TOURNAMENT", "PLAYER", "ODDS"],
#             how="left",
#             indicator=True
#         )
#         new_rows = new_rows[new_rows["_merge"] == "left_only"].drop(columns=["_merge"])

#         if new_rows.empty:
#             print(f"ℹ️ Historical odds for season {season} already exist — no new rows added.")
#         else:
#             new_rows.to_sql("odds", conn, index=False, if_exists="append")
#             print(f"✅ Imported {len(new_rows)} new historical odds for season {season}.")

#     engine.dispose()
#     return new_rows

def import_historical_odds(odds_year: str, season: int, db_path: str) -> pd.DataFrame:
    import pandas as pd
    import numpy as np
    import requests
    import re
    from io import StringIO
    from datetime import datetime
    from sqlalchemy import create_engine
    from utils.schema import odds_table, metadata
    from utils.db_utils import TOURNAMENT_NAME_MAP, PLAYER_NAME_MAP

    url = f"http://golfodds.com/archives-{odds_year}.html"
    response = requests.get(url)
    tables = pd.read_html(StringIO(response.text))
    raw_df = tables[4]

    df = raw_df.dropna(how="all").reset_index(drop=True)
    df.columns = ["PLAYER", "ODDS"]

    # Clean up non-breaking spaces
    df["PLAYER"] = (
        df["PLAYER"]
        .astype(str)
        .str.replace("\xa0", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )

    df.insert(0, "SEASON", season)
    df.insert(1, "TOURNAMENT", value=np.nan)
    df.insert(2, "ENDING_DATE", value=np.nan)

    def parse_ending_date(text):
        text = text.replace("\u2013", "-").replace("–", "-").replace("\xa0", " ")

        # Pattern: "July 30 - August 2, 2015" or "Oct 29 - Nov 1, 2015"
        match = re.search(r"(\w+)\s\d+\s*-\s*(\w+)\s(\d+),\s(\d{4})", text)
        if match:
            month2, day2, year = match.group(2), match.group(3), match.group(4)
            for fmt in ["%B %d, %Y", "%b %d, %Y"]:
                try:
                    return datetime.strptime(f"{month2} {day2}, {year}", fmt).date()
                except ValueError:
                    continue

        # Pattern: "November 21-24, 2024"
        match = re.search(r"(\w+)\s\d+-\d+,\s(\d{4})", text)
        if match:
            month, year = match.group(1), match.group(2)
            day = re.search(r"(\d+)-(\d+)", text).group(2)
            for fmt in ["%B %d, %Y", "%b %d, %Y"]:
                try:
                    return datetime.strptime(f"{month} {day}, {year}", fmt).date()
                except ValueError:
                    continue

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

        # Detect start of a new tournament block
        is_header = (
            pd.isna(df.loc[i, "ODDS"]) and
            pd.isna(df.loc[i + 1, "ODDS"]) and
            re.search(r"\w+\s\d+\s*[-–]\s*(\w+\s)?\d+,\s\d{4}", player_i2)
        )

        if is_header:
            tourn_name = player_i.strip()
            end_date = parse_ending_date(player_i2)

            # Skip cancelled blocks
            if "cancelled" in player_i3:
                i += 4
                continue

            # Avoid duplicate block processing
            if tourn_name == last_tourn_name and end_date == last_end_date:
                i += 1
                continue

            last_tourn_name = tourn_name
            last_end_date = end_date
            i += 4

            # Collect player rows
            while i < len(df) - 2:
                next_i2 = str(df.loc[i + 2, "PLAYER"])
                is_next_header = (
                    pd.isna(df.loc[i, "ODDS"]) and
                    pd.isna(df.loc[i + 1, "ODDS"]) and
                    re.search(r"\w+\s\d+\s*[-–]\s*(\w+\s)?\d+,\s\d{4}", next_i2)
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

    final_df = clean_df[["SEASON", "TOURNAMENT", "ENDING_DATE", "PLAYER", "ODDS", "VEGAS_ODDS"]].copy()

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

# region --- Historical Data
# -----------------------------------------------------
from sqlalchemy import create_engine, text
import pandas as pd

def get_combined_history_seasons(db_path: str, course: str, tournament: str, allowed_seasons: list) -> pd.DataFrame:
    """
    Returns all seasons in which the given course OR tournament was played,
    filtered by allowed seasons. Uses SQLAlchemy.
    """

    engine = create_engine(f"sqlite:///{db_path}")

    query = text("""
        SELECT SEASON, COURSE, TOURN_ID, TOURNAMENT, ENDING_DATE
        FROM tournaments
        WHERE COURSE = :course OR TOURNAMENT = :tournament
    """)

    with engine.begin() as conn:
        df = pd.read_sql(query, conn, params={"course": course, "tournament": tournament})

    df = df[df["SEASON"].isin(allowed_seasons)]
    df = df.drop_duplicates(subset=["SEASON", "TOURNAMENT", "COURSE"])
    df = df.sort_values(by="SEASON")

    engine.dispose()
    print(f"ℹ️ Found {len(df)} relevant tournaments from course or tournament name.")
    return df


from sqlalchemy import create_engine, text
import pandas as pd

def get_cut_and_fedex_history(db_path: str, history_df: pd.DataFrame, window_months: int = 6) -> dict:
    """
    For each row in history_df, compute player-level cuts and FedEx points over
    the past `window_months`, relative to ENDING_DATE.
    """
    from sqlalchemy import create_engine, text
    import pandas as pd
    from datetime import timedelta

    engine = create_engine(f"sqlite:///{db_path}")
    output = {}

    with engine.begin() as conn:
        for _, row in history_df.iterrows():
            end_date = pd.to_datetime(row["ENDING_DATE"]).date()
            season = row["SEASON"]
            start_date = (pd.to_datetime(end_date) - pd.DateOffset(months=window_months)).date()

            query = text("""
                SELECT
                    PLAYER,
                    COUNT(*) as "TOTAL EVENTS PLAYED",
                    COUNT(CASE WHEN POS NOT IN ('CUT', 'W/D') THEN 1 END) AS "CUTS MADE",
                    SUM(CAST(FEDEX_CUP_POINTS AS FLOAT)) AS "FEDEX CUP POINTS"
                FROM tournaments
                WHERE ENDING_DATE BETWEEN :start_date AND DATE(:end_date, '-1 day')
                GROUP BY PLAYER
            """)

            df = pd.read_sql(query, conn, params={
                "start_date": start_date,
                "end_date": end_date
            })

            if not df.empty:
                df["CUT PERCENTAGE"] = ((df["CUTS MADE"] / df["TOTAL EVENTS PLAYED"]) * 100).round(1)
                df = df.sort_values(["FEDEX CUP POINTS", "CUT PERCENTAGE"], ascending=[False, False])
                df["form_density"] = df["FEDEX CUP POINTS"] / df["TOTAL EVENTS PLAYED"]
                df["form_density"] = df["form_density"].round(2)
                df["ENDING_DATE"] = end_date
                df["TOURNAMENT"] = row["TOURNAMENT"]
                output[str(end_date)] = df
            else:
                output[str(season)] = pd.DataFrame()

    engine.dispose()
    return output


from sqlalchemy import create_engine, text
import pandas as pd
from datetime import timedelta
import numpy as np

def get_recent_avg_finish(db_path: str, history_df: pd.DataFrame, window_months: int = 6) -> dict:
    """
    For each ENDING_DATE in history_df, compute average FINAL_POS (i.e. recent form)
    over the past N months. Returns a dict keyed by ENDING_DATE.
    """
    engine = create_engine(f"sqlite:///{db_path}")
    output = {}

    with engine.begin() as conn:
        for _, row in history_df.iterrows():
            end_date = pd.to_datetime(row["ENDING_DATE"]).date()
            tournament = row["TOURNAMENT"]
            start_date = (pd.to_datetime(end_date) - pd.DateOffset(months=window_months)).date()

            query = text("""
                SELECT
                    PLAYER,
                    COUNT(*) AS "TOTAL EVENTS PLAYED",
                    ROUND(AVG(FINAL_POS), 1) AS "RECENT FORM"
                FROM tournaments
                WHERE ENDING_DATE BETWEEN :start_date AND DATE(:end_date, '-1 day')
                GROUP BY PLAYER
            """)

            df = pd.read_sql(query, conn, params={
                "start_date": start_date,
                "end_date": end_date
            })

            if not df.empty:
                df = df.sort_values(by="RECENT FORM", ascending=True).reset_index(drop=True)
                df["adj_form"] = df["RECENT FORM"] / np.log1p(df["TOTAL EVENTS PLAYED"])
                df["adj_form"] = df["adj_form"].round(2)
                df["ENDING_DATE"] = end_date
                df["TOURNAMENT"] = tournament
                output[str(end_date)] = df
            else:
                output[str(end_date)] = pd.DataFrame()

    engine.dispose()
    return output

from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
from datetime import timedelta

def get_course_history(db_path: str, history_df: pd.DataFrame, lookback_years: int = 7) -> dict:
    """
    Computes player average FINAL_POS at the same course over the past X years.
    Adds TOTAL EVENTS and log-adjusted avg (adj_ch).
    """
    engine = create_engine(f"sqlite:///{db_path}")
    output = {}

    with engine.begin() as conn:
        for _, row in history_df.iterrows():
            end_date = pd.to_datetime(row["ENDING_DATE"]).date()
            course = row["COURSE"]
            tournament = row["TOURNAMENT"]
            start_date = (pd.to_datetime(end_date) - pd.DateOffset(years=lookback_years)).date()

            query = text("""
                SELECT
                    PLAYER,
                    COUNT(*) AS "TOTAL EVENTS PLAYED",
                    ROUND(AVG(FINAL_POS), 1) AS "COURSE HISTORY"
                FROM tournaments
                WHERE COURSE = :course
                  AND ENDING_DATE BETWEEN :start_date AND DATE(:end_date, '-1 day')
                GROUP BY PLAYER
            """)

            df = pd.read_sql(query, conn, params={
                "course": course,
                "start_date": start_date,
                "end_date": end_date
            })

            if not df.empty:
                df["adj_ch"] = (df["COURSE HISTORY"] / np.log1p(df["TOTAL EVENTS PLAYED"])).round(2)
                df["ENDING_DATE"] = end_date
                df["COURSE"] = course
                df["TOURNAMENT"] = tournament
                output[str(end_date)] = df
            else:
                output[str(end_date)] = pd.DataFrame()

    engine.dispose()
    return output


# endregion

# region --- Training Dataset
# -----------------------------------------------------

from sqlalchemy import create_engine
import pandas as pd

def build_training_rows(
    db_path: str,
    history_df: pd.DataFrame,
    cuts: dict,
    recent_form: dict,
    course_hist: dict,
    stats_df: pd.DataFrame,
    odds_df: pd.DataFrame
) -> pd.DataFrame:

    engine = create_engine(f"sqlite:///{db_path}")
    all_rows = []

    with engine.begin() as conn:
        for _, row in history_df.iterrows():
            end_date = pd.to_datetime(row["ENDING_DATE"]).date()
            season = row["SEASON"]
            tournament = row["TOURNAMENT"]
            course = row["COURSE"]

            # 1. Base: tournaments for this event
            query = f"""
            SELECT *
            FROM tournaments
            WHERE ENDING_DATE = :end_date
            AND TOURNAMENT = :tourn_name
            """
            tournaments = pd.read_sql(query, conn, params={
                "end_date": end_date,
                "tourn_name": tournament
            })

            if tournaments.empty:
                continue

            # 2. Merge stats (by SEASON + PLAYER)
            temp = tournaments.merge(
                stats_df[stats_df.SEASON == season],
                on=["SEASON", "PLAYER"],
                how="left"
            )

            # 3. Merge odds (by SEASON + PLAYER + TOURNAMENT)
            odds_sub = odds_df[
                (odds_df.SEASON == season) &
                (odds_df.TOURNAMENT == tournament)
            ]
            temp = temp.merge(
                odds_sub[["PLAYER", "VEGAS_ODDS"]],
                on="PLAYER",
                how="left"
            )

            # 4. Merge rolling features (by ENDING_DATE + PLAYER)
            if str(end_date) in cuts:
                temp = temp.merge(cuts[str(end_date)][["PLAYER", "CUT PERCENTAGE", "FEDEX CUP POINTS", "form_density"]], on="PLAYER", how="left")
            if str(end_date) in recent_form:
                temp = temp.merge(recent_form[str(end_date)][["PLAYER", "RECENT FORM", "adj_form"]], on="PLAYER", how="left")
            if str(end_date) in course_hist:
                temp = temp.merge(course_hist[str(end_date)][["PLAYER", "COURSE HISTORY", "adj_ch"]], on="PLAYER", how="left")

            # Keep a copy
            all_rows.append(temp)

    # Concatenate all into one DataFrame
    training_df = pd.concat(all_rows, ignore_index=True)
    engine.dispose()
    return training_df

# endregion