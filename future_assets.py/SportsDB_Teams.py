import dlt
import requests
from typing import List, Dict, Any, Iterator
from dotenv import load_dotenv
import os
import logging
import urllib.parse

# Load environment variables
load_dotenv(dotenv_path="/workspaces/CamOnDagster/.env")

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Base API URL for TheSportsDB
API_URL = "https://www.thesportsdb.com/api/v1/json/3/"

# Raw league names (not URL-encoded)
TOP_LEAGUES = [
    "Australian A-League",        # Australia
    "English Premier League",     # England
    "Spanish La Liga",            # Spain
    "French Ligue 1",             # France
    "German Bundesliga"           # Germany
]

# Fetch teams for a specific league


def get_teams_for_league(league_name: str) -> List[Dict[str, Any]]:
    try:
        encoded_league = urllib.parse.quote(league_name)
        url = f"{API_URL}search_all_teams.php?l={encoded_league}"
        logger.info(f"Fetching teams for league: {league_name}")
        response = requests.get(url)
        response.raise_for_status()

        data = response.json()
        teams = data.get("teams", []) or []
        logger.info(f"{league_name}: Retrieved {len(teams)} teams.")
        return teams
    except requests.RequestException as e:
        logger.error(f"Failed to fetch teams for league '{league_name}': {e}")
        return []

# DLT resource: yields structured data for teams in all top leagues


@dlt.resource(name="teams", write_disposition="merge", primary_key="id_team")
def fetch_teams_data() -> Iterator[Dict[str, Any]]:
    for league in TOP_LEAGUES:
        teams = get_teams_for_league(league)
        for team in teams:
            team["league_name"] = league  # Track source league
            yield team

# DLT source definition


@dlt.source
def teams_pipeline():
    return fetch_teams_data()


# Main entry point
if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="teams_pipeline",
        destination="duckdb",
        dataset_name="sports_db",
        dev_mode=False
    )

    logger.info("Starting DLT pipeline execution...")
    pipeline.run(teams_pipeline())
    logger.info("Pipeline executed successfully. Data loaded into DuckDB.")
