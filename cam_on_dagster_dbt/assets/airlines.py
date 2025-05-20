from dagster import asset, OpExecutionContext
import csv
import dlt
from dlt import current
import os
from dotenv import load_dotenv
from dlt.sources.helpers import requests
from datetime import date
import json

load_dotenv(dotenv_path="/workspaces/CamOnDagster/.env")

# URLs for OpenFlights dataset
URLS = {
    "airlines": "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airlines.dat",
    "airports": "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat",
    "routes": "https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat",
    "planes": "https://raw.githubusercontent.com/jpatokal/openflights/master/data/planes.dat",
}

# Headers for each file based on OpenFlights docs
HEADERS = {
    "airlines": ["Airline ID", "Name", "Alias", "IATA", "ICAO", "Callsign", "Country", "Active"],
    "airports": ["Airport ID", "Name", "City", "Country", "IATA", "ICAO",
                 "Latitude", "Longitude", "Altitude", "Timezone", "DST", "Tz database time zone", "Type", "Source"],
    "routes": ["Airline", "Airline ID", "Source airport", "Source airport ID",
               "Destination airport", "Destination airport ID", "Codeshare", "Stops", "Equipment"],
    "planes": ["Name", "IATA", "ICAO"]
}


def json_converter(o):
    if isinstance(o, date):
        return o.isoformat()
    return str(o)


def create_resource(resource_name, existing_count: int, context: OpExecutionContext):
    @dlt.resource(name=resource_name, write_disposition="replace")
    def resource_fn():

        state = current.source_state().setdefault(resource_name, {
            "processed_records": 0,
            "run_status": "",
            "skipped_rows": 0
        })

        if existing_count == state["processed_records"]:
            context.log.info(
                f"\n🔁 SKIPPED LOAD: `{resource_name}` — No new data.")
            state["run_status"] = "skipped_no_new_data"
            return
        try:
            response = requests.get(URLS[resource_name])
            response.raise_for_status()
            reader = csv.reader(response.iter_lines(decode_unicode=True))

            count = 0
            headers = HEADERS[resource_name]

            skipped = 0
            for row in reader:
                if len(row) != len(headers):
                    # Skip malformed rows
                    skipped += 1
                    continue
                yield dict(zip(headers, row))
                count += 1

            state["processed_records"] = count
            state["run_status"] = "success"
            state["skipped_rows"] = skipped
            context.log.info(
                f"✅ Loaded {count} rows from {resource_name} (skipped {skipped})")
        except Exception as e:
            state["run_status"] = "failed"
            context.log.error(f"❌ Failed to load `{resource_name}`: {e}")
            raise e
    return resource_fn


@dlt.source
def openflights_source(context: OpExecutionContext, row_counts_dict: dict):
    yield create_resource("airlines", row_counts_dict.get("airlines", 0), context)
    yield create_resource("airports", row_counts_dict.get("airports", 0), context)
    yield create_resource("routes", row_counts_dict.get("routes", 0), context)
    yield create_resource("planes", row_counts_dict.get("planes", 0), context)


@asset(group_name="openflights", compute_kind="python")
def openflights_data(context: OpExecutionContext) -> bool:
    pipeline = dlt.pipeline(
        pipeline_name="openflights_pipeline",
        destination=os.getenv("DLT_DESTINATION", "duckdb"),
        dataset_name="openflights",
        dev_mode=False
    )

    row_counts = pipeline.dataset().row_counts().df()
    if row_counts is not None:
        row_counts_dict = dict(
            zip(row_counts["table_name"], row_counts["row_count"]))
    else:
        context.log.warning(
            "⚠️ No tables found yet in dataset — assuming first run.")
        row_counts_dict = {}
    source = openflights_source(context, row_counts_dict)
    try:
        load_info = pipeline.run(source)
        run_statuses = [source.state.get(
            resource, {})["run_status"] for resource in URLS.keys()]

        context.log.info("Airline State Metadata:\n" +
                         json.dumps(source.state, indent=2, default=json_converter))

        if all(s == "skipped_no_new_data" for s in run_statuses):
            context.log.info(
                "\n\n ⏭️ All Cities skipped — no data loaded.")
            return False
        elif all(s == "failed" for s in run_statuses):
            context.log.error(
                "\n\n 💥 All cities failed to load — check API or network.")
            return False

        loaded_count = sum(1 for s in run_statuses if s == "success")
        context.log.info(f"\n\n ✅ Number of cities loaded: {loaded_count}")

        return True
    except Exception as e:
        context.log.error(f"❌ Pipeline failed: {e}")
        return False
    finally:
        context.log.info("Pipeline dropped.")
        pipeline.drop()
