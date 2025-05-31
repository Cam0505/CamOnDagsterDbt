from dagster import asset, OpExecutionContext
import os
# import requests
from dotenv import load_dotenv
from pathlib import Path
import dlt
from datetime import datetime
from zoneinfo import ZoneInfo
import time
import subprocess
from dlt.sources.helpers import requests

load_dotenv(dotenv_path="/workspaces/CamOnDagster/.env")
BASE_URL = "https://api.openuv.io/api/v1/uv"

cities = [
    {"city": "Sydney", "lat": -33.8688, "lng": 151.2093},
    {"city": "Melbourne", "lat": -37.8136, "lng": 144.9631},
    {"city": "Brisbane", "lat": -27.4698, "lng": 153.0251},
    {"city": "Perth", "lat": -31.9505, "lng": 115.8605},
    {"city": "Adelaide", "lat": -34.9285, "lng": 138.6007},
    {"city": "Canberra", "lat": -35.2809, "lng": 149.1300},
    {"city": "Hobart", "lat": -42.8821, "lng": 147.3272},
    {"city": "Darwin", "lat": -12.4634, "lng": 130.8456}
]


def get_dates(context: OpExecutionContext):
    try:
        pipeline = dlt.current.pipeline()
        with pipeline.sql_client() as client:
            result = client.execute_sql(
                f"SELECT date_col FROM public_staging.staging_uv_data_dates")
            return [row[0] for row in result] if result else []
    except Exception as e:
        context.log.info("Failed to retrieve missing dates from the database.")
        return []


def get_uv_data(lat: float, lng: float, dt: datetime, context: OpExecutionContext):
    dt_local = datetime.combine(dt, datetime.min.time(
    ), tzinfo=ZoneInfo("Australia/Sydney")).replace(hour=12)
    headers = {"x-access-token": os.getenv("UV_API_KEY")}
    params = {
        "lat": lat,
        "lng": lng,
        "alt": 100,
        "dt": dt_local.astimezone(ZoneInfo("UTC")).strftime('%Y-%m-%dT%H:%M:%SZ')
    }
    try:
        response = requests.get(BASE_URL, headers=headers,
                                params=params, timeout=10)
        response.raise_for_status()
        return [response.json()]
    except Exception as e:
        context.log.warning(
            f"Failed to fetch UV data for ({lat}, {lng}, {dt}): {e}")
        return []


@dlt.source
def openuv_source(cities: list[dict], dates: list[datetime], context: OpExecutionContext):

    @dlt.resource(name="uv_index", write_disposition="merge", primary_key=["uv_time", "City"])
    def uv_resource():
        context.log.info(f"Using UV_API_KEY: {os.getenv('UV_API_KEY')}")
        for dt in dates:
            for city_info in cities:
                context.log.info(
                    f"Fetching UV for {city_info['city']} on {dt}")
                uv_data = get_uv_data(
                    city_info["lat"], city_info["lng"], dt, context)
                for entry in uv_data:
                    yield {
                        "uv": entry["result"]["uv"],
                        "uv_max": entry["result"]["uv_max"],
                        "uv_time": entry["result"]["uv_time"],
                        "ozone": entry["result"]["ozone"],
                        "City": city_info["city"],
                        "location": {
                            "lat": city_info["lat"],
                            "lng": city_info["lng"]
                        },
                        "timestamp": datetime.now(ZoneInfo("Australia/Sydney")).isoformat()
                    }

    return uv_resource()


@asset(compute_kind="python", group_name="OpenUV", tags={"source": "OpenUV"})
def uv_asset(context: OpExecutionContext) -> bool:
    """Loads UV data from OpenUV API using DLT."""
    context.log.info("🚀 Starting DLT pipeline for OpenUV API")

    pipeline = dlt.pipeline(
        pipeline_name="openuv_pipeline",
        destination=os.getenv("DLT_DESTINATION", "duckdb"),
        dataset_name="uv_data",
        dev_mode=False
    )

    try:
        missing_dates = get_dates(context)
        source = openuv_source(cities, missing_dates, context)
        pipeline.run(source)
        context.log.info("✅ DLT pipeline completed successfully.")
        return True
    except Exception as e:
        context.log.error(f"Failed to run DLT pipeline: {e}")
        return False
    finally:
        # Force exit for stuck threads
        import sys
        sys.stdout.flush()
        sys.stderr.flush()
