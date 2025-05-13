# db_utils.py

# region --- Update Tournament
# -----------------------------------------------------
import requests
import pandas as pd
import sqlite3 as sql
from datetime import datetime
import numpy as np
from numpy import nan
import urllib3

X_API_KEY = "da2-gsrx5bibzbb4njvhl7t37wqyl4"

def update_tournament_results(config: dict, db_path: str, season: int, year: int, verify_ssl=False):
    """Scrapes and updates the tournament results in the SQLite DB."""

    # Extract tournament details
    tourn_id = config["old"]["id"]
    tourn_name = config["old"]["name"]
    course = config["old"]["course"]
    date_str = config["old"]["date"]

    print(f"📦 Preparing payload for tournament ID: {tourn_id}, year: {year}")

    payload = {
        "operationName": "TournamentPastResults",
        "variables": {
            "tournamentPastResultsId": tourn_id,
            "year": year
        },
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

    # API request
    # Suppress SSL warning if SSL verification is disabled
    if not verify_ssl:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
    print("📬 Sending request to PGA Tour API...")
    try:
        response = requests.post(
            "https://orchestrator.pgatour.com/graphql",
            json=payload,
            headers={"x-api-key": X_API_KEY},
            verify=verify_ssl  # TEMPORARY: Disable SSL verification while at work due to corporate proxy issue
        )
        response.raise_for_status()
        print("✅ API request succeeded.")
    except Exception as e:
        print("❌ API request failed:")
        print(e)
        return None

    try:
        json_data = response.json()
        players = json_data["data"]["tournamentPastResults"]["players"]
        print(f"🔍 Found {len(players)} players in response.")
    except Exception as e:
        print("❌ JSON structure unexpected — response may be malformed.")
        raise e

    if not players:
        raise ValueError("No player data found. Check tournament ID and year.")

    # Convert to DataFrame
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

    print("💾 Connecting to database...")
    db = sql.connect(db_path)
    existing = pd.read_sql("SELECT * FROM tournaments", db)

    # Merge & write
    combined = pd.concat([existing, df]).drop_duplicates(["PLAYER", "TOURNAMENT", "ENDING_DATE"], keep="last")
    combined["ENDING_DATE"] = pd.to_datetime(combined["ENDING_DATE"]).dt.date
    combined.to_sql("tournaments", db, index=False, if_exists="replace")
    db.close()

    print(f"✅ Tournament results for '{tourn_name}' added to {db_path}")
    
    return combined


# endregion

# region --- Update Stats

def update_season_stats(stats_year: int, db_path: str, verify_ssl=False) -> pd.DataFrame:
    """Scrapes PGA stat categories for a given year and updates the stats table in the database."""
    import urllib3
    if not verify_ssl:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    import requests
    import pandas as pd
    import sqlite3 as sql
    from numpy import nan

    X_API_KEY = "da2-gsrx5bibzbb4njvhl7t37wqyl4"

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
            "query": "query StatDetails($tourCode: TourCode!, $statId: String!, $year: Int, $eventQuery: StatDetailEventQuery) {\n  statDetails(\n    tourCode: $tourCode\n    statId: $statId\n    year: $year\n    eventQuery: $eventQuery\n  ) {\n    tourCode\n    year\n    displaySeason\n    statId\n    statType\n    tournamentPills {\n      tournamentId\n      displayName\n    }\n    yearPills {\n      year\n      displaySeason\n    }\n    statTitle\n    statDescription\n    tourAvg\n    lastProcessed\n    statHeaders\n    statCategories {\n      category\n      displayName\n      subCategories {\n        displayName\n        stats {\n          statId\n          statTitle\n        }\n      }\n    }\n    rows {\n      ... on StatDetailsPlayer {\n        __typename\n        playerId\n        playerName\n        country\n        countryFlag\n        rank\n        rankDiff\n        rankChangeTendency\n        stats {\n          statName\n          statValue\n          color\n        }\n      }\n      ... on StatDetailTourAvg {\n        __typename\n        displayName\n        value\n      }\n    }\n  }\n}"
        }

        res = requests.post(
            "https://orchestrator.pgatour.com/graphql",
            json=payload,
            headers={"x-api-key": X_API_KEY},
            verify=verify_ssl
        )
        res.raise_for_status()

        json_data = res.json()
        details = json_data.get("data", {}).get("statDetails")

        if not details or "rows" not in details:
            print(f"⚠️ No data found for stat ID {stat_id} in {year}. Skipping.")
            return pd.DataFrame(columns=["PLAYER", "RANK", "VALUE"])

        rows = filter(lambda x: x.get("__typename") == "StatDetailsPlayer", details["rows"])

        table = []
        for x in rows:
            try:
                player = x.get("playerName", "UNKNOWN")
                rank = x.get("rank", None)
                stats = x.get("stats", [])
                value = stats[0]["statValue"] if isinstance(stats, list) and stats else nan
                table.append({"PLAYER": player, "RANK": rank, "VALUE": value})
            except Exception as e:
                print(f"⚠️ Skipping problematic row: {x}")
                print(f"Reason: {e}")

        return pd.DataFrame(table)


    # Build combined stat DataFrame
    base_stat_key = "SG:TTG"
    stat_frames = {}
    for stat_name, stat_id in stat_ids.items():
        df = get_stats(stats_year, stat_id)
        df = df.rename(columns={"RANK": f"{stat_name.replace(':','')}_RANK", "VALUE": stat_name})
        stat_frames[stat_name] = df

    stats = stat_frames[base_stat_key]
    for stat_name, df in stat_frames.items():
        if stat_name == base_stat_key:
            continue
        stats = stats.merge(df, on="PLAYER", how="outer")

    stats.insert(0, "SEASON", stats_year)

    # Merge into database
    conn = sql.connect(db_path)
    existing = pd.read_sql("SELECT * FROM stats", conn)
    combined = pd.concat([existing, stats]).drop_duplicates(subset=["SEASON", "PLAYER"], keep="last")
    combined.to_sql("stats", conn, index=False, if_exists="replace")

    # Return only this season's new data
    query = f"SELECT * FROM stats WHERE SEASON = {stats_year}"
    result = pd.read_sql_query(query, conn)
    conn.close()

    print(f"✅ Stats for season {stats_year} updated in database.")
    return result

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
def clean_odds_names(db_path: str, tournament_map: dict, player_map: dict):
    db = sql.connect(db_path)
    odds = pd.read_sql("SELECT * FROM odds", db)

    odds = odds.replace({"TOURNAMENT": tournament_map})
    odds = odds.replace({"PLAYER": player_map})

    odds["TOURNAMENT"] = odds["TOURNAMENT"].str.replace(r"\s", " ", regex=True)
    odds["PLAYER"] = odds["PLAYER"].str.replace(r"\s", " ", regex=True)

    odds.to_sql("odds", db, index=False, if_exists="replace")
    db.close()

    print("✅ Odds names cleaned and updated.")
# endregion
