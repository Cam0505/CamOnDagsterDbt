from dagster import asset, AssetExecutionContext, Output
import csv
import pandas as pd
import requests
from dotenv import load_dotenv

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


@asset(group_name="openflights", compute_kind="python", name="airlines_asset")
def airlines_asset(context: AssetExecutionContext):
    response = requests.get(URLS["airlines"])
    response.raise_for_status()
    # Convert to list for len()
    reader = list(csv.reader(response.iter_lines(decode_unicode=True)))

    headers = HEADERS["airlines"]
    malformed = 0

    # Compute count before processing for efficiency
    count = len(reader)

    last_event = context.instance.get_latest_materialization_event(
        context.asset_key)
    if last_event and last_event.asset_materialization and last_event.asset_materialization.metadata:
        prev_row_count = last_event.asset_materialization.metadata.get(
            'row_count', 0)
        malformed_rows = last_event.asset_materialization.metadata.get(
            'malformed_rows', 0)
        prev_row_count = getattr(prev_row_count, "value", prev_row_count)
        malformed_rows = getattr(malformed_rows, "value", malformed_rows)
    else:
        prev_row_count = 0
    # I need to implement this, however it's complex, dagster is blood picky
    # If the row count hasn't changed, exit early and do not write to the database
    # if prev_row_count == count:
    #     return Output(
    #         value=None,
    #         metadata={
    #             "row_count": count,
    #             "malformed_rows": malformed_rows,
    #             "headers": headers
    #         }
    #     )

    rows = []
    for row in reader:
        if len(row) != len(headers):
            malformed += 1
            continue
        rows.append(row)

    df = pd.DataFrame(rows, columns=headers)
    return Output(
        value=df,
        metadata={
            "row_count": count,
            "malformed_rows": malformed,
            "headers": headers
        }
    )


@asset(group_name="openflights", compute_kind="python", name="airports_asset")
def airports_asset(context: AssetExecutionContext):
    response = requests.get(URLS["airports"])
    response.raise_for_status()
    # Convert to list for len()
    reader = list(csv.reader(response.iter_lines(decode_unicode=True)))

    headers = HEADERS["airports"]
    malformed = 0

    # Compute count before processing for efficiency
    count = len(reader)

    rows = []
    for row in reader:
        if len(row) != len(headers):
            malformed += 1
            continue
        rows.append(row)

    df = pd.DataFrame(rows, columns=headers)
    return Output(
        value=df,
        metadata={
            "row_count": count,
            "malformed_rows": malformed,
            "headers": headers
        }
    )


@asset(group_name="openflights", compute_kind="python", name="routes_asset")
def routes_asset(context: AssetExecutionContext):
    response = requests.get(URLS["routes"])
    response.raise_for_status()
    # Convert to list for len()
    reader = list(csv.reader(response.iter_lines(decode_unicode=True)))

    headers = HEADERS["routes"]
    malformed = 0

    # Compute count before processing for efficiency
    count = len(reader)

    rows = []
    for row in reader:
        if len(row) != len(headers):
            malformed += 1
            continue
        rows.append(row)

    df = pd.DataFrame(rows, columns=headers)
    return Output(
        value=df,
        metadata={
            "row_count": count,
            "malformed_rows": malformed,
            "headers": headers
        }
    )


@asset(group_name="openflights", compute_kind="python", name="planes_asset")
def planes_asset(context: AssetExecutionContext):
    response = requests.get(URLS["planes"])
    response.raise_for_status()
    # Convert to list for len()
    reader = list(csv.reader(response.iter_lines(decode_unicode=True)))

    headers = HEADERS["planes"]
    malformed = 0

    # Compute count before processing for efficiency
    count = len(reader)

    rows = []
    for row in reader:
        if len(row) != len(headers):
            malformed += 1
            continue
        rows.append(row)

    df = pd.DataFrame(rows, columns=headers)
    return Output(
        value=df,
        metadata={
            "row_count": count,
            "malformed_rows": malformed,
            "headers": headers
        }
    )
