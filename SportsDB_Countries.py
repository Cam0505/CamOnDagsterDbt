import dlt
import requests
from typing import List, Dict, Any, Iterator
from dotenv import load_dotenv
import os
import logging

# Load environment variables
load_dotenv(dotenv_path="/workspaces/CamOnDagster/.env")

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# API URL for TheSportsDB - Countries endpoint
API_URL = "https://www.thesportsdb.com/api/v1/json/3/all_countries.php"

headers = {
    "Content-Type": "application/json"
}

# Fetch data from API: Retrieve all countries


def get_countries() -> List[Dict[str, Any]]:
    try:
        logger.info("Fetching list of countries...")
        response = requests.get(API_URL, headers=headers)
        response.raise_for_status()

        data = response.json()

        countries = data.get('countries', [])
        logger.info(f"Retrieved {len(countries)} countries.")
        return countries
    except requests.RequestException as e:
        logger.error(f"Failed to fetch countries data: {e}")
        return []

# DLT resource: yields structured data for countries


@dlt.resource(name="countries", write_disposition="replace")
def fetch_countries_data() -> Iterator[Dict[str, Any]]:
    countries = get_countries()
    for country in countries:
        yield country

# DLT source definition


@dlt.source
def countries_pipeline():
    return fetch_countries_data()


# Main entry point
if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="countries_pipeline",
        destination="duckdb",
        dataset_name="sports_db",
        dev_mode=False
    )

    logger.info("Starting DLT pipeline execution...")
    pipeline.run(countries_pipeline())
    logger.info("Pipeline executed successfully. Data loaded into DuckDB.")
