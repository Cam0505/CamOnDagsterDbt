from dagster import asset, OpExecutionContext
import os
from dotenv import load_dotenv
import dlt
import subprocess
from typing import Dict, Tuple
from pathlib import Path
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
import json
import time
from dlt.sources.helpers import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv(dotenv_path="/workspaces/CamOnDagster/.env")


cities = {
    "Sydney": {"lat": -33.8688, "lng": 151.2093,
               "timezone": "Australia/Sydney", "country": "Australia"},
    "Melbourne": {"lat": -37.8136, "lng": 144.9631,
                  "timezone": "Australia/Melbourne", "country": "Australia"},
    "Brisbane": {"lat": -27.4698, "lng": 153.0251,
                 "timezone": "Australia/Brisbane", "country": "Australia"},
    "Perth": {"lat": -31.9505, "lng": 115.8605,
              "timezone": "Australia/Perth", "country": "Australia"},
    "Adelaide": {"lat": -34.9285, "lng": 138.6007,
                 "timezone": "Australia/Adelaide", "country": "Australia"},
    "Canberra": {"lat": -35.2809,
                 "lng": 149.1300, "timezone": "Australia/Sydney", "country": "Australia"},
    "Hobart": {"lat": -42.8821, "lng": 147.3272,
               "timezone": "Australia/Hobart", "country": "Australia"},
    "Darwin": {"lat": -12.4634,
               "lng": 130.8456, "timezone": "Australia/Darwin", "country": "Australia"},
    "Cairns": {"lat": -16.92366, "lng": 145.76613,
               "timezone": "Australia/Brisbane", "country": "Australia"},
    "Alice Springs": {"lat": -23.697479, "lng": 133.883621,
                      "timezone": "Australia/Darwin", "country": "Australia"},
    "Albany": {"lat": -35.02692, "lng": 117.88369,
               "timezone": "Australia/Perth", "country": "Australia"},
    "Palmerston North": {"lat": -40.3563556918218, "lng": 175.61113357543945,
                         "timezone": "Pacific/Auckland", "country": "New Zealand"},
    "Wellington": {"lat": -41.2865, "lng": 174.7762,
                   "timezone": "Pacific/Auckland", "country": "New Zealand"},
    "Auckland": {"lat": -36.8485, "lng": 174.7633,
                 "timezone": "Pacific/Auckland", "country": "New Zealand"},
    "Christchurch": {"lat": -43.5321, "lng": 172.6362,
                     "timezone": "Pacific/Auckland", "country": "New Zealand"}
}

today = datetime.now(ZoneInfo("Australia/Sydney")).date()
end_date = today - timedelta(days=2)
# Last 3 years worth of data, don't need this now
start_date = end_date - timedelta(days=3 * 365)

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"


def json_converter(o):
    if isinstance(o, date):
        return o.isoformat()
    return str(o)


def get_city_date_stats(context) -> Dict[str, Dict[str, date]]:
    try:
        pipeline = dlt.current.pipeline()
        with pipeline.sql_client() as client:
            result = client.execute_sql("""
            SELECT
                City,
                MIN(date::date) as min_date,
                MAX(date::date) as max_date,
                COUNT(*) as count
            FROM weather_data.daily_weather
            GROUP BY City""")
            if not result:
                context.log.info("No results returned from SQL query.")
                return {}

            return {
                row[0]: {
                    "min_date": row[1],
                    "max_date": row[2],
                    "count": row[3]
                }
                for row in result
            }
    except Exception as e:
        context.log.warning(
            f"⚠️ Could not return query data: {str(e)}")
        return {}  # Assume table doesn't exist yet


def get_weather_data(lat: float, lng: float, start_date: date, end_date: date, timezone: str):
    return requests.get(BASE_URL,
                        params={
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
                        )


def split_into_yearly_chunks(start_date: date, end_date: date):
    chunks = []
    current = start_date
    while current <= end_date:
        year_end = min(datetime(current.year + 1, 1, 1).date() -
                       timedelta(days=1), end_date)
        chunks.append((current, year_end))
        current = year_end + timedelta(days=1)
    return chunks


def fetch_city_chunk_data(city: str, city_info: dict, city_start: date, end_date: date) -> Tuple[str, list]:
    records = []
    success = False
    for chunk_start, chunk_end in split_into_yearly_chunks(city_start, end_date):
        response = get_weather_data(
            lat=city_info["lat"],
            lng=city_info["lng"],
            start_date=chunk_start,
            end_date=chunk_end,
            timezone=city_info["timezone"]
        )
        data = response.json()
        if not data or "daily" not in data:
            continue
        daily_data = data["daily"]
        for i in range(len(daily_data["time"])):
            success = True
            records.append({
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
            })
    return city, records if success else []


@dlt.source
def openmeteo_source(cities: dict, base_start_date: date, end_date: date, context: OpExecutionContext):

    @dlt.resource(name="daily_weather", write_disposition="merge", primary_key=["date", "City"])
    def weather_resource():
        state = dlt.current.source_state().setdefault("Weather", {
            "city_date": {},
            "city_status": {},
            "last_run_date": {"Min": str(end_date), "Max": str(end_date)},
            "last_run_status": None
        })

        city_stats = get_city_date_stats(context)
        all_dates = []
        futures = {}

        with ThreadPoolExecutor(max_workers=4) as executor:
            for city, city_info in cities.items():
                city_start = base_start_date

                if city in city_stats:
                    # Database Values
                    max_date = city_stats[city]['max_date']
                    min_date = city_stats[city]['min_date']
                    # Whats being passed in
                    expected_days = (end_date - base_start_date).days + 1
                    # In the DB
                    existing_days = (max_date - min_date).days + 1

                    if existing_days >= expected_days and max_date >= end_date:
                        context.log.info(
                            f"✅ Skipping {city}: full data available ({existing_days}/{expected_days})")
                        state["city_status"][city] = "skipped"
                        continue
                    # Given the Database has data upto: max_date, you would start fetching from: max_date + timedelta(days=1)
                    city_start = max(
                        max_date + timedelta(days=1), base_start_date)
                    context.log.info(
                        f"🔄 Updating {city}: found {existing_days}/{(end_date - min_date).days + 1} days, starting from {city_start}"
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

                futures[executor.submit(
                    fetch_city_chunk_data, city, city_info, city_start, end_date)] = city

                for future in as_completed(futures):
                    city = futures[future]
                    records = future.result()[1]
                    if records:
                        for record in records:
                            yield record
                        state["city_status"][city] = "success"
                        state["city_date"][city] = {
                            "start": records[0]["date"],
                            "end": records[-1]["date"]
                        }
                        all_dates.append(
                            date.fromisoformat(records[0]["date"]))
                        all_dates.append(
                            date.fromisoformat(records[-1]["date"]))
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
                         json.dumps(outcome_data, indent=2, default=json_converter))

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
def dbt_meteo_data(context: OpExecutionContext, openmeteo_asset: bool) -> None:
    """Runs the dbt command after loading the data from OpenMeteo API."""

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
            # shell=True,
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
