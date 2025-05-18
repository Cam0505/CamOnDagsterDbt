from dagster import asset, OpExecutionContext
import os
import requests
from dotenv import load_dotenv
import dlt
import subprocess
from typing import Dict
from pathlib import Path
import duckdb
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
import json
import time

load_dotenv(dotenv_path="/workspaces/CamOnDagster/.env")

cities = {
    "Sydney": {"lat": -33.8688, "lng": 151.2093,
               "timezone": "Australia/Sydney"},
    "Melbourne": {"lat": -37.8136, "lng": 144.9631,
                  "timezone": "Australia/Melbourne"},
    "Brisbane": {"lat": -27.4698, "lng": 153.0251,
                 "timezone": "Australia/Brisbane"},
    "Perth": {"lat": -31.9505, "lng": 115.8605,
              "timezone": "Australia/Perth"},
    "Adelaide": {"lat": -34.9285, "lng": 138.6007,
                 "timezone": "Australia/Adelaide"},
    "Canberra": {"lat": -35.2809,
                 "lng": 149.1300, "timezone": "Australia/Sydney"},
    "Hobart": {"lat": -42.8821, "lng": 147.3272,
               "timezone": "Australia/Hobart"},
    "Darwin": {"lat": -12.4634,
               "lng": 130.8456, "timezone": "Australia/Darwin"}
}
today = datetime.now(ZoneInfo("Australia/Sydney")).date()
end_date = today - timedelta(days=2)
# Last 3 years worth of data, don't need this now
start_date = end_date - timedelta(days=3 * 365)

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"


def get_city_date_stats(context) -> Dict[str, Dict[str, date]]:
    db_path = os.getenv("DESTINATION__DUCKDB__CREDENTIALS")
    if not db_path:
        raise ValueError(
            "Missing DESTINATION__DUCKDB__CREDENTIALS in environment.")
    conn = duckdb.connect(database=db_path, read_only=True)
    try:
        result = conn.execute("""
            SELECT
                City,
                MIN(date::date) as min_date,
                MAX(date::date) as max_date,
                COUNT(*) as count
            FROM weather_data.daily_weather
            GROUP BY City
        """).fetchall()
        return {
            row[0]: {
                "min_date": row[1],
                "max_date": row[2],
                "count": row[3]
            }
            for row in result
        }
    except duckdb.CatalogException as e:
        context.log.info(f"No Table Exists: {e}")
        return {}  # Table doesn't exist
    except Exception as e:
        context.log.info(f"Exception: {e}")
        return {}
    finally:
        conn.close()


def get_weather_data(lat: float, lng: float, start_date: date, end_date: date, timezone: str, context: OpExecutionContext):
    params = {
        "latitude": lat,
        "longitude": lng,
        "start_date": start_date.strftime('%Y-%m-%d'),
        "end_date": end_date.strftime('%Y-%m-%d'),
        "daily": ",".join([
            "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
            "precipitation_sum", "windspeed_10m_max", "windgusts_10m_max",
            "sunshine_duration", "uv_index_max"
        ]),
        "timezone": timezone
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        context.log.warning(
            f"Failed to fetch weather data for ({lat}, {lng}) from {start_date} to {end_date}: {e}")
        return None


def split_into_yearly_chunks(start_date: date, end_date: date):
    chunks = []
    current = start_date
    while current <= end_date:
        year_end = min(datetime(current.year + 1, 1, 1).date() -
                       timedelta(days=1), end_date)
        chunks.append((current, year_end))
        current = year_end + timedelta(days=1)
    return chunks


@dlt.source
def openmeteo_source(cities: dict, base_start_date: date, end_date: date, context: OpExecutionContext):

    @dlt.resource(name="daily_weather", write_disposition="merge", primary_key=["date", "City"])
    def weather_resource():
        state = dlt.current.source_state().setdefault("Weather", {
            "city_date": {},
            "city_status": {},
            "last_run_date": {"Min": end_date, "Max": end_date},
            "last_run_status": None
        })

        city_stats = get_city_date_stats(context)
        all_dates = []
        # context.log.info(f"Existing city stats: {json.dumps(city_stats, indent=2, default=str)}")

        for city, city_info in cities.items():
            city_start = base_start_date

            if city in city_stats:
                max_date = city_stats[city]['max_date']
                min_date = city_stats[city]['min_date']
                # Whats being passed in
                expected_days = (end_date - base_start_date).days + 1
                # In the DB
                existing_days = (max_date - min_date).days + 1

                if existing_days >= expected_days:
                    context.log.info(
                        f"✅ Skipping {city}: full data available ({existing_days}/{expected_days})")
                    state["city_status"][city] = "skipped"
                    continue
                # Given the Database has data upto: max_date, you would start fetching from: max_date + timedelta(days=1)
                city_start = max(max_date + timedelta(days=1), base_start_date)
                context.log.info(
                    f"🔄 Updating {city}: found {existing_days}/{expected_days} days, starting from {city_start}"
                )

            else:
                context.log.info(
                    f"🆕 New city: {city}, fetching from {city_start}")
            # If the new fetch date is beyond the hard limit (No newer data than 2 days ago, set as global var)
            if city_start > end_date:
                context.log.info(
                    f"📭 No missing data range to fetch for {city}")
                state["city_status"][city] = "skipped"
                continue

            success = False
            for chunk_start, chunk_end in split_into_yearly_chunks(city_start, end_date):
                data = get_weather_data(
                    lat=city_info["lat"],
                    lng=city_info["lng"],
                    start_date=chunk_start,
                    end_date=chunk_end,
                    timezone=city_info["timezone"],
                    context=context
                )
                if not data or "daily" not in data:
                    continue
                daily_data = data["daily"]
                for i in range(len(daily_data["time"])):
                    success = True
                    yield {
                        "date": daily_data["time"][i],
                        "City": city,
                        "temperature_max": daily_data["temperature_2m_max"][i],
                        "temperature_min": daily_data["temperature_2m_min"][i],
                        "temperature_mean": daily_data["temperature_2m_mean"][i],
                        "precipitation_sum": daily_data["precipitation_sum"][i],
                        "windspeed_max": daily_data["windspeed_10m_max"][i],
                        "windgusts_max": daily_data["windgusts_10m_max"][i],
                        "sunshine_duration": daily_data["sunshine_duration"][i],
                        "uv_index_max": daily_data["uv_index_max"][i],
                        "location": {
                            "lat": city_info["lat"],
                            "lng": city_info["lng"]
                        },
                        "timestamp": datetime.now(ZoneInfo(city_info["timezone"])).replace(microsecond=0).isoformat()
                    }
            if success:
                state["city_status"][city] = "success"
                state["city_date"][city] = {
                    "start": city_start,
                    "end": end_date
                }
                all_dates.append(city_start)
                all_dates.append(end_date)
            else:
                state["city_status"][city] = "failed"

        if all_dates:
            state["last_run_date"]["Min"] = str(min(all_dates))
            state["last_run_date"]["Max"] = str(max(all_dates))
            state["last_run_status"] = "success"
        else:
            state["last_run_status"] = "no_data"
    return weather_resource()


@asset(compute_kind="python", group_name="Open_Meteo", tags={"source": "Open_Meteo"})
def openmeteo_asset(context: OpExecutionContext) -> bool:

    context.log.info("Starting DLT pipeline...")
    pipeline = dlt.pipeline(
        pipeline_name="openmeteo_pipeline",
        destination=os.getenv("DLT_DESTINATION", "duckdb"),
        dataset_name="weather_data"
    )

    source = openmeteo_source(
        cities=cities,
        base_start_date=start_date,
        end_date=end_date,
        context=context
    )

    try:
        pipeline.run(source)
        outcome_data = source.state.get('Weather', {})
        context.log.info("Weather State Metadata:\n" +
                         json.dumps(outcome_data, indent=2))

        statuses = [outcome_data.get("city_status", {}).get(
            city, '') for city in cities.keys()]
        if all(s == "skipped" for s in statuses):
            context.log.info(
                "\n\n ⏭️ All Cities skipped — no data loaded.")
            return False
        elif all(s == "failed" for s in statuses):
            context.log.error(
                "\n\n 💥 All cities failed to load — check API or network.")
            return False

        loaded_count = sum(1 for s in statuses if s == "success")
        context.log.info(f"\n\n ✅ Number of cities loaded: {loaded_count}")

        return True

    except Exception as e:
        context.log.error(f"\n\n ❌ Pipeline run failed: {e}")
        return False


@asset(deps=["openmeteo_asset"], group_name="Open_Meteo", tags={"source": "Open_Meteo"})
def dbt_meteo_data(context: OpExecutionContext, get_geo_data: bool) -> None:
    """Runs the dbt command after loading the data from Geo API."""

    if not openmeteo_asset:
        context.log.warning(
            "\n⚠️  WARNING: DBT SKIPPED\n"
            "📉 No data was loaded from OpenMeteo API.\n"
            "🚫 Skipping dbt run.\n"
            "----------------------------------------"
        )
        return

    DBT_PROJECT_DIR = Path("/workspaces/CamOnDagster/dbt").resolve()
    context.log.info(f"DBT Project Directory: {DBT_PROJECT_DIR}")

    start = time.time()
    try:
        result = subprocess.run(
            ["dbt", "build", "--select", "source:weather+"],
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
