from dagster import asset, OpExecutionContext
import os
import requests
from dotenv import load_dotenv
from pathlib import Path
import dlt
import duckdb
import time
import subprocess

load_dotenv(dotenv_path="/workspaces/CamOnDagster/.env")


@asset(compute_kind="python")
def get_geo_data(context: OpExecutionContext) -> bool:
    """
    This function fetches data from the GeoAPI and stores it in a local file.
    """

    db_path = os.getenv("DESTINATION__DUCKDB__CREDENTIALS")
    if not db_path:
        raise ValueError(
            "Missing DuckDB path in DESTINATION__DUCKDB__CREDENTIALS")

    conn = duckdb.connect(database=db_path)
    EXPECTED_CITY_COUNT = 126
    row_count = 0
    try:
        count = conn.execute(
            f"SELECT COUNT(*) FROM geo_data.geo_cities"
        ).fetchone()
        if count is not None:
            row_count = count[0]  # Extract the count from the tuple
        else:
            row_count = -1  # Handle case where table is empty or doesn't exist
    except Exception:
        row_count = -1   # Table probably doesn't exist
    finally:
        conn.close()

    # Prevent Reloading if the data is already present, Unlikely to change in Source API
    if row_count == EXPECTED_CITY_COUNT:
        context.log.info(
            f"GeoAPI data already loaded. Skipping data fetch. Row count: {row_count}, Expected: {EXPECTED_CITY_COUNT}")
        return False

    # API credentials and URL for GeoNames
    USERNAME = os.getenv("GEONAMES_USERNAME")
    if not USERNAME:
        raise ValueError("Missing GEONAMES_USERNAME in environment.")

    BASE_URL = "http://api.geonames.org/citiesJSON"
    DETAILS_URL = "http://api.geonames.org/getJSON"

    def make_request_with_retries(url, params, max_retries=5, backoff_factor=2):
        for attempt in range(max_retries):
            try:
                prepared = requests.Request(
                    "GET", url, params=params).prepare()
                print("DEBUG URL:", prepared.url)

                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()
                return response.json()
            except requests.RequestException as e:
                wait_time = backoff_factor ** attempt
                context.log.warning(
                    f"Attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
        context.log.error(f"All retries failed for params: {params}")
        return {}

    def fetch_city_details(geoname_id):
        params = {
            "geonameId": geoname_id,
            "username": USERNAME
        }
        return make_request_with_retries(DETAILS_URL, params)

    def fetch_cities(country_code):
        max_rows = 100
        total_fetched = 0

        context.log.info(f"Starting fetch for country: {country_code}")

        params = {
            "formatted": "true",
            "lat": "0",
            "lng": "0",
            "maxRows": max_rows,
            "lang": "en",
            "username": USERNAME
        }

        if country_code == "AU":
            params.update({"north": "-10.0", "south": "-44.0",
                          "east": "155.0", "west": "112.0"})
        elif country_code == "NZ":
            params.update({"north": "-33.0", "south": "-47.0",
                          "east": "180.0", "west": "166.0"})
        elif country_code == "GB":
            params.update({"north": "60.0", "south": "49.0",
                          "east": "1.0", "west": "-8.0"})
        elif country_code == "CA":
            params.update({"north": "83.0", "south": "42.0",
                          "east": "-52.0", "west": "-140.0"})

        cities_data = make_request_with_retries(
            BASE_URL, params).get("geonames", [])

        for city in cities_data:
            total_fetched += 1
            details = fetch_city_details(city.get("geonameId")) or {}

            yield {
                "city_id": city.get("geonameId"),
                "city": city.get("name"),
                "latitude": city.get("lat"),
                "longitude": city.get("lng"),
                "country": details.get("countryName") or city.get("countryName"),
                "country_code": country_code,
                "region": details.get("adminName1"),
                "region_code": details.get("adminCode1"),
                "continent": details.get("continentCode")
            }

        context.log.info(
            f"Total cities fetched for {country_code}: {total_fetched}")

    @dlt.source
    def geo_source():

        @dlt.resource(name="geo_cities", write_disposition="merge", primary_key="city_id")
        def cities():
            for country in ["AU", "NZ", "GB", "CA"]:
                try:
                    yield from fetch_cities(country)
                except Exception as e:
                    context.log.error(
                        f"Error while processing country {country}: {e}")

        return [cities]

    context.log.info("Starting DLT pipeline...")

    pipeline = dlt.pipeline(
        pipeline_name="geo_cities_pipeline",
        destination="duckdb",
        dataset_name="geo_data",
        dev_mode=False
    )

    try:
        context.log.info("Pipeline started.")
        load_info = pipeline.run(geo_source())
        context.log.info(f"Pipeline finished. Load info: {load_info}")
        return True
    except Exception as e:
        context.log.exception(
            "Unhandled exception in GeoAPI pipeline execution.")
        return False


@asset(deps=["get_geo_data"])
def dbt_geo_data(context: OpExecutionContext, get_geo_data: bool) -> None:
    """Runs the dbt command after loading the data from Geo API."""
    if not get_geo_data:
        context.log.info("Geo data not updated, skipping dbt build.")
        return

    DBT_PROJECT_DIR = Path("/workspaces/CamOnDagster/dbt").resolve()
    context.log.info(f"DBT Project Directory: {DBT_PROJECT_DIR}")

    start = time.time()
    try:
        result = subprocess.run(
            "dbt build --select source:geo+",
            shell=True,
            cwd=DBT_PROJECT_DIR,
            capture_output=True,
            text=True,
            check=True
        )
        duration = round(time.time() - start, 2)
        context.log.info(f"dbt build completed in {duration}s")
        context.log.info(result.stdout)
    except subprocess.CalledProcessError as e:
        context.log.error(f"dbt build failed:\n{e.stdout}\n{e.stderr}")
        raise
