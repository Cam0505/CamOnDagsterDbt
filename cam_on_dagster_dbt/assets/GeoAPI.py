from dagster import asset, OpExecutionContext
import os
import requests
from dotenv import load_dotenv
from pathlib import Path
import dlt
import time
import subprocess
import duckdb
import json

load_dotenv(dotenv_path="/workspaces/CamOnDagster/.env")
COUNTRIES = ["AU", "NZ", "GB", "CA"]


def get_existing_count(country_code: str, context) -> int:
    db_path = os.getenv("DESTINATION__DUCKDB__CREDENTIALS")
    if not db_path:
        raise ValueError(
            "Missing DESTINATION__DUCKDB__CREDENTIALS in environment.")

    con = duckdb.connect(database=db_path)
    try:
        result = con.execute(
            "SELECT COUNT(*) FROM geo_data.geo_cities WHERE country_code = ?",
            [country_code]).fetchone()
        return result[0] if result else 0
    except Exception as e:
        context.log.warn(f"Failed to get row count for geo_cities: {e}")
        return 0  # Table might not exist yet
    finally:
        con.close()


@dlt.source
def geo_source(context: OpExecutionContext):
    @dlt.resource(name="geo_cities", write_disposition="merge", primary_key="city_id")
    def cities():
        # Initialize state at the start of each run
        state = dlt.current.source_state().setdefault("geo_cities", {
            "processed_records": {},
            "country_status": {}
        })

        # context.log.info(f"Current state at the beginning of the run: {state}")

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

            # Country-specific bounding boxes
            bboxes = {
                "AU": {"north": "-10.0", "south": "-44.0", "east": "155.0", "west": "112.0"},
                "NZ": {"north": "-33.0", "south": "-47.0", "east": "180.0", "west": "166.0"},
                "GB": {"north": "60.0", "south": "49.0", "east": "1.0", "west": "-8.0"},
                "CA": {"north": "83.0", "south": "42.0", "east": "-52.0", "west": "-140.0"}
            }

            if country_code in bboxes:
                params.update(bboxes[country_code])
            try:
                cities_data = make_request_with_retries(
                    BASE_URL, params).get("geonames", [])
            except Exception as e:
                context.log.error(
                    f"Failed to fetch cities for {country_code}: {e}")
                state["country_status"][country_code] = "failed"
                raise
            try:
                database_rowcount = get_existing_count(country_code, context)
            except Exception as e:
                context.log.error(f"Failed to connect for {country_code}: {e}")
                state["country_status"][country_code] = "database_error"
                raise

            current_count = len(cities_data)

            previous_count = state["processed_records"].get(country_code, 0)

            if database_rowcount < previous_count:
                context.log.info(
                    f"⚠️ GeoAPI data for `{country_code}` row count dropped from {previous_count} to {database_rowcount}. Forcing reload.")
                state["country_status"][country_code] = "database_row_count"
            elif (current_count == previous_count):
                context.log.info(f"\n🔁 SKIPPED LOAD:\n"
                                 f"📅 Previous Run for {country_code}: {previous_count}\n"
                                 f"📦 API Cities for {country_code}: {current_count}\n"
                                 f"⏳ No new data for {country_code}. Skipping... \n"
                                 f"{'-'*45}")
                state["country_status"][country_code] = "skipped_no_new_data"
                return

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

            # Update the state with the number of records processed in this run
            state["processed_records"][country_code] = total_fetched
            state["country_status"][country_code] = "success"
            context.log.info(
                f"Total cities fetched for {country_code}: {total_fetched}")

        try:
            for country in COUNTRIES:
                try:
                    yield from fetch_cities(country)
                except Exception as e:
                    context.log.error(
                        f"Error while processing country {country}: {e}")
                    raise
            context.log.info(f"Current state after successful run: {state}")
        except Exception as e:
            context.log.error(f"Processing failed: {e}")
            raise
    return cities


@asset(compute_kind="python", group_name="Geo", tags={"source": "Geo"})
def get_geo_data(context: OpExecutionContext) -> bool:

    context.log.info("Starting DLT pipeline...")
    pipeline = dlt.pipeline(
        pipeline_name="geo_cities_pipeline",
        destination=os.getenv("DLT_DESTINATION", "duckdb"),
        dataset_name="geo_data",
        dev_mode=False
    )

    source = geo_source(context)
    try:
        load_info = pipeline.run(source)

        outcome_data = source.state.get(
            'geo_cities', {}).get("country_status", {})

        context.log.info("Country Status:\n" +
                         json.dumps(outcome_data, indent=2))

        statuses = [outcome_data.get(resource, 0) for resource in COUNTRIES]

        if any(s == "success" for s in statuses):
            context.log.info(f"Pipeline Load Info: {load_info}")
            return True
        elif all(s == "skipped_no_new_data" for s in statuses):
            return False
        else:
            context.log.error(
                "💥  Pipeline Failures — check Logic, API or network.")
            return False

    except Exception as e:
        context.log.error(f"❌ Pipeline run failed: {e}")
        return False


@asset(deps=["get_geo_data"], group_name="Geo", tags={"source": "Geo"})
def dbt_geo_data(context: OpExecutionContext, get_geo_data: bool) -> None:
    """Runs the dbt command after loading the data from Geo API."""

    if not get_geo_data:
        context.log.warning(
            "\n⚠️  WARNING: DBT SKIPPED\n"
            "📉 No data was loaded from GeoAPI.\n"
            "🚫 Skipping dbt run.\n"
            "----------------------------------------"
        )
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
