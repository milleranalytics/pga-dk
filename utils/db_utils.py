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

# region --- Odds Dictionaries

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
# def clean_odds_names(db_path: str, tournament_map: dict, player_map: dict):
#     db = sql.connect(db_path)
#     odds = pd.read_sql("SELECT * FROM odds", db)

#     odds = odds.replace({"TOURNAMENT": tournament_map})
#     odds = odds.replace({"PLAYER": player_map})

#     odds["TOURNAMENT"] = odds["TOURNAMENT"].str.replace(r"\s", " ", regex=True)
#     odds["PLAYER"] = odds["PLAYER"].str.replace(r"\s", " ", regex=True)

#     odds.to_sql("odds", db, index=False, if_exists="replace")
#     db.close()

#     print("✅ Odds names cleaned and updated.")
# endregion
