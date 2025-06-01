from dagster import asset, AssetExecutionContext
import os
import pandas as pd
from dotenv import load_dotenv
from dlt.pipeline.exceptions import PipelineNeverRan
from dlt.destinations.exceptions import DatabaseUndefinedRelation
import dlt
from typing import Dict, Tuple
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
import json
from dlt.sources.helpers import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from path_config import ENV_FILE, DLT_PIPELINE_DIR

load_dotenv(dotenv_path=ENV_FILE)


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
# Set start_date to 1st of January 2021
start_date = date(2021, 1, 1)

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"


def json_converter(o):
    if isinstance(o, date):
        return o.isoformat()
    return str(o)


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


def fetch_city_chunk_data(city: str, city_info: dict, city_start: date, end_date: date, context) -> Tuple[str, list]:
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
        response.raise_for_status()
        if response.status_code != 200:
            context.log.error(
                f"🌐 Failed to fetch data for {city}: {response.status_code} {response.text}")
            continue
        data = response.json()
        if not data or "daily" not in data:
            context.log.warning(
                f"⚠️ No data found for {city} between {chunk_start} and {chunk_end}")
            continue
        daily_data = data["daily"]
        for i in range(len(daily_data["time"])):
            success = True
            date_val = daily_data["time"][i]
            if not isinstance(date_val, date):
                date_val = date.fromisoformat(date_val)
            records.append({
                "date": date_val,
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
def openmeteo_source(cities: dict, base_start_date: date, end_date: date, row_max_min: Dict[str, Dict[str, date]], context: AssetExecutionContext):

    @dlt.resource(name="daily_weather", write_disposition="merge", primary_key=["date", "City"])
    def weather_resource():
        state = dlt.current.source_state().setdefault("Weather", {
            "city_date": {},
            "city_status": {},
            "last_run_date": {"Min": str(end_date), "Max": str(end_date)},
            "last_run_status": None
        })

        all_dates = []
        futures = {}

        with ThreadPoolExecutor(max_workers=5) as executor:
            for city, city_info in cities.items():
                city_start = base_start_date

                if city in row_max_min:
                    max_date = row_max_min[city]['max_date']
                    min_date = row_max_min[city]['min_date']
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
                    fetch_city_chunk_data, city, city_info, city_start, end_date, context)] = city

            for future in as_completed(futures):
                city = futures[future]
                try:
                    records = future.result()[1]
                except Exception as e:
                    context.log.error(f"Failed fetching data for {city}: {e}")
                    state["city_status"][city] = "failed"
                    continue
                if records:
                    yield records
                    state["city_status"][city] = "success"
                    state["city_date"][city] = {
                        "start": records[0]["date"],
                        "end": records[-1]["date"]
                    }
                    all_dates.append(
                        records[0]["date"])
                    all_dates.append(
                        records[-1]["date"])
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
def openmeteo_asset(context: AssetExecutionContext) -> bool:

    context.log.info("Starting DLT pipeline...")
    pipeline = dlt.pipeline(
        pipeline_name="openmeteo_pipeline",
        destination=os.getenv("DLT_DESTINATION", "motherduck"),
        pipelines_dir=str(DLT_PIPELINE_DIR),
        dataset_name="weather_data"
    )

    try:
        dataset = pipeline.dataset()["daily_weather"].df()
        if dataset is not None:
            row_max_min = dataset.groupby("city").agg(
                min_date=("date", "min"), max_date=("date", "max")).reset_index()
            row_max_min["min_date"] = pd.to_datetime(
                row_max_min["min_date"]).dt.date
            row_max_min["max_date"] = pd.to_datetime(
                row_max_min["max_date"]).dt.date
            row_max_min_dict = {
                str(k): v for k, v in (
                    row_max_min
                    .set_index("city")[["min_date", "max_date"]]
                    .to_dict(orient="index")
                ).items()
            }
            context.log.info(f"Grouped Min and Max:\n{row_max_min}")
    except PipelineNeverRan:
        context.log.warning(
            "⚠️ No previous runs found for this pipeline. Assuming first run.")
        row_max_min_dict = {}
    except DatabaseUndefinedRelation:
        context.log.warning(
            "⚠️ Table Doesn't Exist. Assuming truncation.")
        row_max_min_dict = {}

    source = openmeteo_source(
        cities=cities,
        base_start_date=start_date,
        end_date=end_date,
        row_max_min=row_max_min_dict,
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


@asset(deps=["openmeteo_asset"], group_name="Open_Meteo",
       tags={"source": "Open_Meteo"}, required_resource_keys={"dbt"})
def dbt_meteo_data(context: AssetExecutionContext, openmeteo_asset: bool) -> None:
    """Runs the dbt command after loading the data from OpenMeteo API."""

    if not openmeteo_asset:
        context.log.warning(
            "\n⚠️  WARNING: DBT SKIPPED\n"
            "📉 No data was loaded from OpenMeteo API.\n"
            "🚫 Skipping dbt run.\n"
            "----------------------------------------"
        )
        return

    try:
        invocation = context.resources.dbt.cli(
            ["build", "--select", "source:meals+"]
        )

        # Wait for dbt to finish and get the full stdout log
        invocation.wait()
        return
    except Exception as e:
        context.log.error(f"dbt build failed:\n{e}")
        raise
