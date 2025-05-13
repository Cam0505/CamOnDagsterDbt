import dlt
import requests
from typing import List, Dict, Any, Iterator
from dotenv import load_dotenv
import os
import logging
import duckdb

# Load environment variables
load_dotenv(dotenv_path="/workspaces/CamOnDagster/.env")

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Base API URL for TheSportsDB
API_URL = "https://www.thesportsdb.com/api/v1/json/3/"

# Fetch players for a given team


def get_players_for_team(team_id: str) -> List[Dict[str, Any]]:
    try:
        logger.info(f"Fetching players for team ID: {team_id}")
        response = requests.get(
            f"{API_URL}lookup_all_players.php", params={"id": team_id})
        response.raise_for_status()
        data = response.json()
        return data.get("player", []) or []
    except requests.RequestException as e:
        logger.error(f"Failed to fetch players for team ID '{team_id}': {e}")
        return []

# DLT resource: players (fetching team IDs from DuckDB)


@dlt.resource(name="players", write_disposition="merge", primary_key=["id_player", "str_player"])
def fetch_players_data() -> Iterator[Dict[str, Any]]:
    # Connect to DuckDB to fetch team IDs
    db_path = os.getenv("DESTINATION__DUCKDB__CREDENTIALS")
    if not db_path:
        raise ValueError(
            "Missing DuckDB path in DESTINATION__DUCKDB__CREDENTIALS")

    connection = duckdb.connect(database=db_path, read_only=True)
    # Query to get all team IDs from the DuckDB table
    team_ids = connection.execute(
        "SELECT id_team FROM sports_db.teams").fetchall()
    print(f"Found {len(team_ids)} team IDs:")

    for team_id_tuple in team_ids:
        team_id = team_id_tuple[0]  # Extract team_id from tuple
        players = get_players_for_team(team_id)
        logger.info(f"{team_id}: Retrieved {len(players)} players.")
        for player in players:
            print(player["idPlayer"], player.get("strPlayer"))
            # Attach the team_id to the player data
            player["team_id"] = team_id
            yield player

    # Close the connection
    connection.close()

# DLT source


@dlt.source
def football_pipeline():
    return fetch_players_data()


# Run pipeline
if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="football_pipeline",
        destination="duckdb",
        dataset_name="sports_db",
        dev_mode=False
    )

    logger.info("Starting DLT pipeline execution...")
    pipeline.run(football_pipeline())
    logger.info("Pipeline executed successfully. Data loaded into DuckDB.")
