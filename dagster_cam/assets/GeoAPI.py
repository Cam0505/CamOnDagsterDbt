from dagster import asset, AssetExecutionContext
import os
from dotenv import load_dotenv
import dlt
from dlt.pipeline.exceptions import PipelineNeverRan
from dlt.destinations.exceptions import DatabaseUndefinedRelation
import json
from dlt.sources.helpers.requests import get
from path_config import ENV_FILE, DLT_PIPELINE_DIR, DBT_DIR


load_dotenv(dotenv_path=ENV_FILE)
COUNTRIES = ["AU", "NZ", "GB", "CA"]


@dlt.source
def geo_source(context: AssetExecutionContext, row_counts_dict: dict):
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

        def fetch_city_details(geoname_id):
            params = {
                "geonameId": geoname_id,
                "username": USERNAME
            }
            return get(DETAILS_URL, params=params).json()

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
                response = get(BASE_URL, params=params)
                response_json = response.json()
                # context.log.info(
                #     f"Raw API response for {country_code}: {json.dumps(response_json, indent=2)}")
                cities_data = response_json.get("geonames", [])
                # context.log.info(
                #     f"Fetched {len(cities_data)} cities for {country_code}")
            except Exception as e:
                context.log.error(
                    f"Failed to fetch cities for {country_code}: {e}")
                state["country_status"][country_code] = "failed"
                raise

            database_rowcount = row_counts_dict.get(country_code, 0)

            current_count = len(cities_data)

            previous_count = state["processed_records"].get(country_code, 0)

            if database_rowcount < previous_count or database_rowcount == 0:
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
                context.log.info(
                    f"Processing city: {city.get('name')} ({city.get('geonameId')}) in {country_code}")
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


@asset(compute_kind="python", group_name="Geo", tags={"source": "Geo"}, io_manager_key=None)
def get_geo_data(context: AssetExecutionContext) -> bool:

    context.log.info("Starting DLT pipeline...")
    pipeline = dlt.pipeline(
        pipeline_name="geo_cities_pipeline",
        destination=os.getenv("DLT_DESTINATION", "motherduck"),
        pipelines_dir=str(DLT_PIPELINE_DIR),
        dataset_name="geo_data",
        dev_mode=False
    )

    try:
        dataset = pipeline.dataset()["geo_cities"].df()
        if dataset is not None:
            row_counts = dataset.groupby(
                "country_code").size().reset_index(name="count")
            context.log.info(f"Grouped Row Counts:\n{row_counts}")
    except PipelineNeverRan:
        context.log.warning(
            "⚠️ No previous runs found for this pipeline. Assuming first run.")
        row_counts = None
    except DatabaseUndefinedRelation:
        context.log.warning(
            "⚠️ Table Doesn't Exist. Assuming deletion.")
        row_counts = None

    if row_counts is not None:
        row_counts_dict = dict(
            zip(row_counts["country_code"], row_counts["count"]))
    else:
        context.log.warning(
            "⚠️ No tables found yet in dataset — assuming first run.")
        row_counts_dict = {}

    source = geo_source(context, row_counts_dict=row_counts_dict)
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


@asset(deps=["get_geo_data"], group_name="Geo",
       tags={"source": "Geo"}, required_resource_keys={"dbt"}, io_manager_key=None)
def dbt_geo_data(context: AssetExecutionContext, get_geo_data: bool) -> None:
    """Runs the dbt command after loading the data from Geo API."""

    if not get_geo_data:
        context.log.warning(
            "\n⚠️  WARNING: DBT SKIPPED\n"
            "📉 No data was loaded from GeoAPI.\n"
            "🚫 Skipping dbt run.\n"
            "----------------------------------------"
        )
        return

    try:
        invocation = context.resources.dbt.cli(
            ["build", "--select", "source:geo+"]
        )

        # Wait for dbt to finish and get the full stdout log
        invocation.wait()
        return
    except Exception as e:
        context.log.error(f"dbt build failed:\n{e}")
        raise
