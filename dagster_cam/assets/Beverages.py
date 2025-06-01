from dagster import asset, AssetExecutionContext
import os
import requests
from dlt.sources.helpers import requests as dlt_requests
from dotenv import load_dotenv
import dlt
import time
from path_config import ENV_FILE, DLT_PIPELINE_DIR


load_dotenv(dotenv_path=ENV_FILE)
TABLE_PARAMS = {
    "beverages": ("c=list", "strCategory"),
    "glasses": ("g=list", "strGlass"),
    "ingredients": ("i=list", "strIngredient1"),
    "alcoholic": ("a=list", "strAlcoholic")
}


def fetch_and_extract(table: str) -> list:
    """
    Fetch data from TheCocktailDB API based on table type, and normalize to a list of values.

    Args:
        table (str): One of "beverages", "glasses", "ingredients", "alcoholic".

    Returns:
        List[str]: Extracted list of values.
    """
    API_KEY = os.getenv('BEVERAGE_API_KEY')
    if not API_KEY:
        raise ValueError(
            "Environment variable BEVERAGE_API_KEY is not set or empty.")

    if table not in TABLE_PARAMS:
        raise ValueError(
            f"Unsupported table: {table}. Valid options: {list(TABLE_PARAMS.keys())}")

    param, field = TABLE_PARAMS[table]
    url = f"https://www.thecocktaildb.com/api/json/v2/{API_KEY}/list.php?{param}"

    response = dlt_requests.get(url)
    response.raise_for_status()  # Raise exception on error
    data = response.json()

    # Find the first key containing a list of dicts
    for key, value in data.items():
        if isinstance(value, list) and all(isinstance(item, dict) for item in value):
            return [item.get(field) for item in value if field in item]

    return []


def create_dimension_resource(table_name, config, values, context):
    @dlt.resource(name=config["resource_name"], write_disposition="replace")
    def resource_func():

        API_KEY = os.getenv('BEVERAGE_API_KEY')
        if not API_KEY:
            raise ValueError(
                "Environment variable BEVERAGE_API_KEY is not set or empty.")

        state = dlt.current.source_state().setdefault(config["resource_name"], {
            "processed_records": 0,
            "last_run_status": None
        })

        total_records = 0

        for value in values:
            url = f"https://www.thecocktaildb.com/api/json/v2/{API_KEY}/filter.php?{config['query_param']}={value}"
            try:
                response = requests.get(url)
                response.raise_for_status()  # Raise exception on error
                drinks = response.json()["drinks"]
                if not drinks:
                    context.log.warning(
                        f"No drinks found for {config['query_param']}={value}")
                    continue
                for drink in drinks:
                    if isinstance(drink, dict):  # Ensure it's a dictionary
                        drink[config["source_key"]] = value
                        yield drink
                        total_records += 1
            except Exception as e:
                context.log.warning(
                    f"❌ Failed to fetch drinks for value '{value}': {e}")
                state["last_run_status"] = "failed"
                return
        # Check Previous State:
        previous_value = state.get("processed_records", 0)
        if total_records == previous_value:
            context.log.info(
                f"🔁 SKIPPED LOAD for {config['resource_name']}:\n"
                f"📅 Previous: {previous_value}\n"
                f"📦 Current: {total_records}\n"
                f"⏳ No new data for {config['resource_name']}. Skipping..."
            )
            state["last_run_status"] = "skipped_no_new_data"
            return
        else:
            state["processed_records"] = total_records
            state["last_run_status"] = "success"

    return resource_func


@dlt.source
def dimension_data_source(context: AssetExecutionContext):

    DIMENSION_CONFIG = {
        "ingredients": {
            "sql_column": "strIngredient1",
            "query_param": "i",
            "source_key": "source_ingredient",
            "resource_name": "ingredients_table"
        },
        "alcoholic": {
            "sql_column": "strAlcoholic",
            "query_param": "a",
            "source_key": "source_alcohol_type",
            "resource_name": "alcoholic_table"
        },
        "beverages": {
            "sql_column": "strCategory",
            "query_param": "c",
            "source_key": "source_beverage_type",
            "resource_name": "beverages_table"
        },
        "glasses": {
            "sql_column": "strGlass",
            "query_param": "g",
            "source_key": "source_glass",
            "resource_name": "glass_table"
        }
    }
    for table_name, config in DIMENSION_CONFIG.items():
        context.log.info(f"Creating resource: {table_name}")
        values = fetch_and_extract(table_name)
        yield create_dimension_resource(table_name, config, values, context)


@asset(compute_kind="python", group_name="Beverages", tags={"source": "Beverages"})
def dimension_data(context) -> bool:

    context.log.info("Starting DLT pipeline...")
    pipeline = dlt.pipeline(
        pipeline_name="beverage_pipeline",
        destination=os.getenv("DLT_DESTINATION", "duckdb"),
        dataset_name="beverage_data",
        pipelines_dir=str(DLT_PIPELINE_DIR),
        dev_mode=False
    )

    source = dimension_data_source(context)
    # run pipeline
    try:
        load_info = pipeline.run(source)
        context.log.info(f"✅ Load successful: {load_info}")
        return bool(load_info)
    except Exception as e:
        context.log.error(f"❌ Pipeline run failed: {e}")
        return False


@asset(compute_kind="python", deps=["dimension_data"], group_name="Beverages", tags={"source": "Beverages"})
def beverage_fact_data(context, dimension_data: bool) -> bool:
    # return False
    if not dimension_data:
        context.log.warning(
            "\n⚠️  WARNING: dimension_data SKIPPED\n"
            "📉 No data was loaded from dimension_data.\n"
            "🚫 Skipping beverage_fact_data run.\n"
            "----------------------------------------"
        )
        return False

    @dlt.resource(name="consumption", write_disposition="append")
    def beverages_api():
        API_KEY = os.getenv("BEVERAGE_API_KEY")
        if not API_KEY:
            raise ValueError(
                "Environment variable BEVERAGE_API_KEY is not set or empty.")

        url = f"https://www.thecocktaildb.com/api/json/v2/{API_KEY}/randomselection.php"

        for i in range(10):
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                drinks = response.json().get("drinks", [])

                if not drinks:
                    context.log.warning(
                        f"No drinks returned in iteration {i+1}")
                    continue

                yield drinks

                time.sleep(0.2)  # Prevent throttling
            except requests.RequestException as e:
                context.log.error(
                    f"Request failed on iteration {i+1}: {e}", exc_info=True)
            except Exception as e:
                context.log.error(
                    f"Unexpected error on iteration {i+1}: {e}", exc_info=True)

    @dlt.source
    def alcoholic_beverages():
        return beverages_api()

    try:
        pipeline = dlt.pipeline(
            pipeline_name="beverage_pipeline",
            destination=os.getenv("DLT_DESTINATION", "motherduck"),
            dataset_name="beverage_data",
            pipelines_dir=str(DLT_PIPELINE_DIR),
            dev_mode=False
        )

        load_info = pipeline.run(alcoholic_beverages())
        context.log.info(f"DLT pipeline run complete: {load_info}")
        return bool(load_info)

    except Exception as e:
        context.log.error("DLT pipeline run failed", exc_info=True)
        raise


@asset(deps=["beverage_fact_data"], group_name="Beverages",
       tags={"source": "Beverages"},
       required_resource_keys={"dbt"})
def dbt_beverage_data(context: AssetExecutionContext, beverage_fact_data: bool):
    """Runs the dbt command after loading the data from Beverage API."""
    # return False
    if not beverage_fact_data:
        context.log.warning(
            "\n⚠️  WARNING: beverage_fact_data SKIPPED\n"
            "📉 No data was loaded from beverage_fact_data.\n"
            "🚫 Skipping DBT Build.\n"
            "----------------------------------------"
        )
        return False

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
