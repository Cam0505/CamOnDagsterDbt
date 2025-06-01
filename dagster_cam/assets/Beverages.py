from dagster import asset, AssetExecutionContext, Output
import os
import requests
from dlt.sources.helpers import requests as dlt_requests
from dotenv import load_dotenv
import pandas as pd
import time
import json
from datetime import datetime, timedelta
from path_config import ENV_FILE, DLT_PIPELINE_DIR, REQUEST_CACHE_DIR
from helper_functions import sanitize_filename

load_dotenv(dotenv_path=ENV_FILE)

API_KEY = os.getenv("BEVERAGE_API_KEY")
if not API_KEY:
    raise ValueError("Environment variable BEVERAGE_API_KEY is not set.")

DIMENSION_CONFIG = {
    "ingredients": {
        "list_api": ("i=list", "strIngredient1"),
        "query_param": "i",
        "source_key": "source_ingredient",
        "resource_name": "ingredients_table",
        "primary_key": ["id_drink", "source_ingredient"]
    },
    "alcoholic": {
        "list_api": ("a=list", "strAlcoholic"),
        "query_param": "a",
        "source_key": "source_alcohol_type",
        "resource_name": "alcoholic_table",
        "primary_key": ["id_drink"]
    },
    "beverages": {
        "list_api": ("c=list", "strCategory"),
        "query_param": "c",
        "source_key": "source_beverage_type",
        "resource_name": "beverages_table",
        "primary_key": ["id_drink"]
    },
    "glasses": {
        "list_api": ("g=list", "strGlass"),
        "query_param": "g",
        "source_key": "source_glass",
        "resource_name": "glasses_table",
        "primary_key": ["id_drink"]
    }
}


def fetch_and_extract(table: str, config: dict, context) -> list:
    # Check if the cache directory exists, if not, create it
    REQUEST_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    cache_file = REQUEST_CACHE_DIR / f"{table}.json"

    param, field = config["list_api"]

    # Check if the cache file exists and is less than 74 hours old
    if cache_file.exists():
        context.log.info(
            f"✅ Using cached response for table: {table} Param: {param} field: {field}")
        file_mtime = datetime.fromtimestamp(cache_file.stat().st_mtime)
        if datetime.now() - file_mtime < timedelta(hours=72):
            with open(cache_file, "r") as f:
                data = json.load(f)
                for key, value in data.items():
                    if isinstance(value, list) and all(isinstance(item, dict) for item in value):
                        return [item.get(field) for item in value if field in item]

    url = f"https://www.thecocktaildb.com/api/json/v2/{API_KEY}/list.php?{param}"

    response = dlt_requests.get(url)
    response.raise_for_status()  # Raise exception on error
    data = response.json()

    # Cache the response
    with open(cache_file, "w") as f:
        json.dump(data, f, indent=2)

    # Find the first key containing a list of dicts
    for key, value in data.items():
        if isinstance(value, list) and all(isinstance(item, dict) for item in value):
            return [item.get(field) for item in value if field in item]

    return []


def resource_dim_request_cache(resource, query_param, value, context):
    REQUEST_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    cache_file = REQUEST_CACHE_DIR / \
        f"{resource}_{query_param}_{sanitize_filename(value)}.json"

    # Check if the cache file exists and is less than 72 hours old
    if cache_file.exists():
        # logger.info(f"✅ Using cached response for {query_param}={sanitize_filename(value)}")
        file_mtime = datetime.fromtimestamp(cache_file.stat().st_mtime)
        if datetime.now() - file_mtime < timedelta(hours=72):
            with open(cache_file, "r") as f:
                return json.load(f)
    else:
        context.log.info(
            f"❌ No cache found for {query_param}={value}, fetching from API...")

    url = f"https://www.thecocktaildb.com/api/json/v2/{API_KEY}/filter.php?{query_param}={value}"

    try:
        response = dlt_requests.get(url)
        response.raise_for_status()  # Raise exception on error
        data = response.json()["drinks"]
    except Exception as e:
        context.log.warning(
            f"❌ Failed to fetch drinks for value '{value}': {e}")
        return []

    with open(cache_file, "w") as f:
        json.dump(data, f, indent=2)

    return data


@asset(compute_kind="python", group_name="Beverages", tags={"source": "Beverages"})
def ingredients_table(context: AssetExecutionContext) -> Output:

    config = DIMENSION_CONFIG["ingredients"]

    values = fetch_and_extract("ingredients", config, context)
    context.log.info(f"Creating resource: ingredients")
    drink_list = []
    for value in values:
        try:
            drinks = resource_dim_request_cache(
                config['resource_name'], config['query_param'], value, context)
        except Exception as e:
            context.log.error(
                f"❌ Failed to fetch drinks for {config['query_param']}={value}: {e}")
            continue

        if not drinks or drinks == 'None Found':
            # context.log.warning(
            #     f"No drinks found for {config['query_param']}={value}")
            continue

        for drink in drinks:
            if isinstance(drink, dict):
                drink[config["source_key"]] = value
                drink_list.append(drink)
            else:
                context.log.warning(f"Skipping non-dict drink: {drink!r}")

    df = pd.DataFrame(data=drink_list)

    return Output(
        df,
        metadata={
            "row_count": len(df),
            "columns": ", ".join(df.columns),
        }
    )


@asset(compute_kind="python", group_name="Beverages", tags={"source": "Beverages"})
def alcoholic_table(context: AssetExecutionContext) -> Output:

    config = DIMENSION_CONFIG["alcoholic"]

    values = fetch_and_extract("alcoholic", config, context)
    context.log.info(f"Creating resource: alcoholic")
    drink_list = []
    for value in values:
        try:
            drinks = resource_dim_request_cache(
                config['resource_name'], config['query_param'], value, context)
        except Exception as e:
            context.log.error(
                f"❌ Failed to fetch drinks for {config['query_param']}={value}: {e}")
            continue

        if not drinks or drinks == 'None Found':
            # context.log.warning(
            #     f"No drinks found for {config['query_param']}={value}")
            continue

        for drink in drinks:
            if isinstance(drink, dict):
                drink[config["source_key"]] = value
                drink_list.append(drink)
            else:
                context.log.warning(f"Skipping non-dict drink: {drink!r}")

    df = pd.DataFrame(data=drink_list)

    return Output(
        df,
        metadata={
            "row_count": len(df),
            "columns": ", ".join(df.columns),
        }
    )


@asset(compute_kind="python", group_name="Beverages", tags={"source": "Beverages"})
def beverages_table(context: AssetExecutionContext) -> Output:

    config = DIMENSION_CONFIG["beverages"]

    values = fetch_and_extract("beverages", config, context)
    context.log.info(f"Creating resource: beverages")
    drink_list = []
    for value in values:
        try:
            drinks = resource_dim_request_cache(
                config['resource_name'], config['query_param'], value, context)
        except Exception as e:
            context.log.error(
                f"❌ Failed to fetch drinks for {config['query_param']}={value}: {e}")
            continue

        if not drinks or drinks == 'None Found':
            # context.log.warning(
            #     f"No drinks found for {config['query_param']}={value}")
            continue

        for drink in drinks:
            if isinstance(drink, dict):
                drink[config["source_key"]] = value
                drink_list.append(drink)
            else:
                context.log.warning(f"Skipping non-dict drink: {drink!r}")

    df = pd.DataFrame(data=drink_list)

    return Output(
        df,
        metadata={
            "row_count": len(df),
            "columns": ", ".join(df.columns),
        }
    )


@asset(compute_kind="python", group_name="Beverages", tags={"source": "Beverages"})
def glass_table(context: AssetExecutionContext) -> Output:

    config = DIMENSION_CONFIG["glasses"]

    values = fetch_and_extract("glasses", config, context)
    context.log.info(f"Creating resource: glasses")
    drink_list = []
    for value in values:
        try:
            drinks = resource_dim_request_cache(
                config['resource_name'], config['query_param'], value, context)
        except Exception as e:
            context.log.error(
                f"❌ Failed to fetch drinks for {config['query_param']}={value}: {e}")
            continue

        if not drinks or drinks == 'None Found':
            # context.log.warning(
            #     f"No drinks found for {config['query_param']}={value}")
            continue

        for drink in drinks:
            if isinstance(drink, dict):
                drink[config["source_key"]] = value
                drink_list.append(drink)
            else:
                context.log.warning(f"Skipping non-dict drink: {drink!r}")

    df = pd.DataFrame(data=drink_list)

    return Output(
        df,
        metadata={
            "row_count": len(df),
            "columns": ", ".join(df.columns),
        }
    )


@asset(compute_kind="python", deps=["glass_table", "beverages_table",
                                    "alcoholic_table", "ingredients_table"],
       group_name="Beverages", tags={"source": "Beverages"})
def beverage_fact_data(context, glass_table: pd.DataFrame, beverages_table: pd.DataFrame, alcoholic_table: pd.DataFrame, ingredients_table: pd.DataFrame) -> Output:
    if glass_table.empty and beverages_table.empty and alcoholic_table.empty and ingredients_table.empty:
        context.log.warning(
            "\n⚠️  WARNING: dimension_data SKIPPED\n"
            "📉 No data was loaded from dimension_data.\n"
            "🚫 Skipping beverage_fact_data run.\n"
            "----------------------------------------"
        )
        return Output(
            pd.DataFrame(),
            metadata={
                "row_count": 0,
                "columns": "None"
            }
        )

    url = f"https://www.thecocktaildb.com/api/json/v2/{API_KEY}/randomselection.php"

    all_drinks = []
    for i in range(10):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            drinks = response.json().get("drinks", [])

            if not drinks:
                context.log.warning(
                    f"No drinks returned in iteration {i+1}")
                continue

            all_drinks.extend(drinks)

            time.sleep(0.2)  # Prevent throttling
        except requests.RequestException as e:
            context.log.error(
                f"Request failed on iteration {i+1}: {e}", exc_info=True)
        except Exception as e:
            context.log.error(
                f"Unexpected error on iteration {i+1}: {e}", exc_info=True)
    if not all_drinks:
        context.log.warning(
            "\n⚠️  WARNING: No drinks found\n"
            "📉 Skipping beverage_fact_data run.\n"
            "----------------------------------------"
        )
        return Output(
            pd.DataFrame(),
            metadata={
                "row_count": 0,
                "columns": "None"
            }
        )

    context.log.info(f"Total drinks fetched: {len(all_drinks)}")
    df = pd.DataFrame(all_drinks)

    return Output(
        df,
        metadata={
            "row_count": len(df),
            "columns": ", ".join(df.columns),
        }
    )


@asset(deps=["beverage_fact_data"], group_name="Beverages",
       tags={"source": "Beverages"}, required_resource_keys={"dbt"}, io_manager_key=None)
def dbt_beverage_data(context: AssetExecutionContext, beverage_fact_data: pd.DataFrame) -> None:
    """Runs the dbt command after loading the data from Beverage API."""
    # return False
    if beverage_fact_data.empty:
        context.log.warning(
            "\n⚠️  WARNING: beverage_fact_data SKIPPED\n"
            "📉 No data was loaded from beverage_fact_data.\n"
            "🚫 Skipping DBT Build.\n"
            "----------------------------------------"
        )
        return

    try:
        invocation = context.resources.dbt.cli(
            ["build", "--select", "source:beverages+"]
        )

        # Wait for dbt to finish and get the full stdout log
        invocation.wait()
        return
    except Exception as e:
        context.log.error(f"dbt build failed:\n{e}")
        raise
