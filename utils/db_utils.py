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
    'Fabian Gomez'        : 'Fabián Gómez'
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

    final_df = clean_df[["SEASON", "TOURNAMENT", "ENDING_DATE", "PLAYER", "ODDS", "VEGAS_ODDS"]].copy()
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
    For each row in history_df, compute player-level cuts, FedEx points, and consecutive cuts
    over the past `window_months`, relative to ENDING_DATE.
    """
    from sqlalchemy import create_engine, text
    import pandas as pd
    import numpy as np

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
                    ENDING_DATE,
                    POS,
                    CAST(FEDEX_CUP_POINTS AS FLOAT) AS FEDEX_CUP_POINTS
                FROM tournaments
                WHERE ENDING_DATE BETWEEN :start_date AND DATE(:end_date, '-1 day')
            """)

            df = pd.read_sql(query, conn, params={
                "start_date": start_date,
                "end_date": end_date
            })

            if df.empty:
                output[str(end_date)] = pd.DataFrame()
                continue

            # Work with DB columns as-is
            df = df.sort_values(by=["PLAYER", "ENDING_DATE"])
            df["MADE_CUT"] = ~df["POS"].isin(["CUT", "W/D"])

            # Aggregate stats per player (still DB-style column names)
            agg_df = df.groupby("PLAYER").agg(
                TOTAL_EVENTS_PLAYED=("POS", "count"),
                CUTS_MADE=("MADE_CUT", "sum"),
                FEDEX_CUP_POINTS=("FEDEX_CUP_POINTS", "sum")
            ).reset_index()

            # Feature engineering (snake_case)
            agg_df["CUT_PERCENTAGE"] = ((agg_df["CUTS_MADE"] / agg_df["TOTAL_EVENTS_PLAYED"]) * 100).round(1)
            agg_df["form_density"] = (agg_df["FEDEX_CUP_POINTS"] / agg_df["TOTAL_EVENTS_PLAYED"]).round(2)

            # Compute consecutive cuts streak
            def count_consecutive_cuts(player_df):
                cuts = player_df["MADE_CUT"].tolist()[::-1]
                count = 0
                for cut in cuts:
                    if cut:
                        count += 1
                    else:
                        break
                return count

            streaks = df.groupby("PLAYER").apply(count_consecutive_cuts).reset_index(name="CONSECUTIVE_CUTS")

            result_df = agg_df.merge(streaks, on="PLAYER", how="left")
            result_df["ENDING_DATE"] = end_date
            result_df["TOURNAMENT"] = tournament

            output[str(end_date)] = result_df

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
    from sqlalchemy import create_engine, text
    import pandas as pd
    import numpy as np

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
                    COUNT(*) AS TOTAL_EVENTS_PLAYED,
                    ROUND(AVG(FINAL_POS), 1) AS RECENT_FORM
                FROM tournaments
                WHERE ENDING_DATE BETWEEN :start_date AND DATE(:end_date, '-1 day')
                GROUP BY PLAYER
            """)

            df = pd.read_sql(query, conn, params={
                "start_date": start_date,
                "end_date": end_date
            })

            if df.empty:
                output[str(end_date)] = pd.DataFrame()
                continue

            df = df.sort_values(by="RECENT_FORM", ascending=True).reset_index(drop=True)
            df["adj_form"] = (df["RECENT_FORM"] / np.log1p(df["TOTAL_EVENTS_PLAYED"])).round(2)
            df["ENDING_DATE"] = end_date
            df["TOURNAMENT"] = tournament
            output[str(end_date)] = df

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
    from sqlalchemy import create_engine, text
    import pandas as pd
    import numpy as np

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
                    COUNT(*) AS TOTAL_EVENTS_PLAYED,
                    ROUND(AVG(FINAL_POS), 1) AS COURSE_HISTORY
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

            if df.empty:
                output[str(end_date)] = pd.DataFrame()
                continue

            df["adj_ch"] = (df["COURSE_HISTORY"] / np.log1p(df["TOTAL_EVENTS_PLAYED"])).round(2)
            df["ENDING_DATE"] = end_date
            df["COURSE"] = course
            df["TOURNAMENT"] = tournament
            output[str(end_date)] = df

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
    course_hist: dict
) -> pd.DataFrame:

    # Load full stats from the database
    def load_all_stats(db_path):
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            stats = pd.read_sql("SELECT * FROM stats", conn)
        engine.dispose()
        return stats

    # Load full odds from the database
    def load_all_odds(db_path):
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            odds_df = pd.read_sql("SELECT * FROM odds", conn)
        engine.dispose()
        return odds_df

    stats_df = load_all_stats(db_path)
    odds_df = load_all_odds(db_path)

    all_rows = []
    engine = create_engine(f"sqlite:///{db_path}")

    with engine.begin() as conn:
        for _, row in history_df.iterrows():
            end_date = pd.to_datetime(row["ENDING_DATE"]).date()
            season = row["SEASON"]
            tournament = row["TOURNAMENT"]

            # 1. Base tournament results
            base_df = pd.read_sql(
                """
                SELECT *
                FROM tournaments
                WHERE ENDING_DATE = :end_date AND TOURNAMENT = :tourn_name
                """,
                conn,
                params={"end_date": end_date, "tourn_name": tournament}
            )

            if base_df.empty:
                continue

            # 2. Merge Stats (by SEASON + PLAYER)
            stats_sub = stats_df[stats_df["SEASON"] == season].copy()
            base_df["PLAYER"] = base_df["PLAYER"].astype(str).str.strip()
            stats_sub["PLAYER"] = stats_sub["PLAYER"].astype(str).str.strip()
            temp = base_df.merge(stats_sub, on=["SEASON", "PLAYER"], how="left")

            # 3. Merge Odds (by ENDING_DATE + PLAYER)
            odds_sub = odds_df[
                (pd.to_datetime(odds_df["ENDING_DATE"]).dt.date == end_date) &
                (odds_df["TOURNAMENT"] == tournament)
            ].copy()
            odds_sub["PLAYER"] = odds_sub["PLAYER"].astype(str).str.strip()
            temp["PLAYER"] = temp["PLAYER"].astype(str).str.strip()
            temp = temp.merge(
                odds_sub[["PLAYER", "VEGAS_ODDS"]],
                on="PLAYER",
                how="left"
            )

            # 4. Merge Rolling Features (by ENDING_DATE + PLAYER)
            date_key = str(end_date)

            if date_key in cuts:
                cuts[date_key]["PLAYER"] = cuts[date_key]["PLAYER"].astype(str).str.strip()
                temp = temp.merge(
                    cuts[date_key][["PLAYER", "CUT_PERCENTAGE", "FEDEX_CUP_POINTS", "form_density", "CONSECUTIVE_CUTS"]],
                    on="PLAYER", how="left"
                )

            if date_key in recent_form:
                recent_form[date_key]["PLAYER"] = recent_form[date_key]["PLAYER"].astype(str).str.strip()
                temp = temp.merge(
                    recent_form[date_key][["PLAYER", "RECENT_FORM", "adj_form"]],
                    on="PLAYER", how="left"
                )

            if date_key in course_hist:
                course_hist[date_key]["PLAYER"] = course_hist[date_key]["PLAYER"].astype(str).str.strip()
                temp = temp.merge(
                    course_hist[date_key][["PLAYER", "COURSE_HISTORY", "adj_ch"]],
                    on="PLAYER", how="left"
                )

            all_rows.append(temp)

    engine.dispose()

    if all_rows:
        training_df = pd.concat(all_rows, ignore_index=True)

        # Drop irrelevant columns
        columns_to_drop = [
            "ROUNDS:1", "ROUNDS:2", "ROUNDS:3", "ROUNDS:4",
            "OFFICIAL_MONEY",
            "FEDEX_CUP_POINTS_x",  # from tournament result
            "TOURN_ID"
        ]
        training_df.drop(columns=[col for col in columns_to_drop if col in training_df.columns], inplace=True)

        # Rename rolling FedEx column if needed
        if "FEDEX_CUP_POINTS_y" in training_df.columns:
            training_df.rename(columns={"FEDEX_CUP_POINTS_y": "FEDEX_CUP_POINTS"}, inplace=True)

        # Add TOP_20 binary label
        training_df["TOP_20"] = (training_df["FINAL_POS"] <= 20).astype(int)
    else:
        training_df = pd.DataFrame()

    return training_df

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

    return odds_df

# endregion

import pandas as pd
from sqlalchemy import create_engine

def load_all_stats(db_path: str) -> pd.DataFrame:
    """
    Loads the full 'stats' table from the SQLite database.
    Returns a DataFrame with all seasons and players.
    """
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        stats_df = pd.read_sql("SELECT * FROM stats", conn)
    engine.dispose()
    return stats_df