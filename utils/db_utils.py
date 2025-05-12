# db_utils.py

# region --- Update Tournament
# -----------------------------------------------------
import requests
import pandas as pd
import sqlite3 as sql
from datetime import datetime
import numpy as np
from numpy import nan

X_API_KEY = "da2-gsrx5bibzbb4njvhl7t37wqyl4"

def update_tournament_results(config: dict, db_path: str, season: int, year: int):
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
    print("📬 Sending request to PGA Tour API...")
    try:
        response = requests.post(
            "https://orchestrator.pgatour.com/graphql",
            json=payload,
            headers={"x-api-key": X_API_KEY},
            verify=True  # TEMPORARY: Disable SSL verification
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
        print("❌ Failed to parse JSON response:")
        print("Raw response:", response.text)
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