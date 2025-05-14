from dagster import asset, OpExecutionContext
import os
import requests
from pathlib import Path
from dotenv import load_dotenv
import dlt
import time
import subprocess

load_dotenv(dotenv_path="/workspaces/CamOnDagster/.env")


def create_resource(table_name, param, value_key, context):
    @dlt.resource(name=table_name, write_disposition="replace")
    def resource_func():
        API_KEY = os.getenv('BEVERAGE_API_KEY')
        if not API_KEY:
            raise ValueError(
                "Environment variable BEVERAGE_API_KEY is not set or empty.")

        url = f"https://www.thecocktaildb.com/api/json/v2/{API_KEY}/list.php?{param}"
        response = requests.get(url)
        response.raise_for_status()
        drinks = response.json().get("drinks", [])

        state = dlt.current.source_state().setdefault(table_name, {
            "processed_records": 0,
            "last_run_status": None,
            "values": []
        })

        current_list = [d.get(value_key) for d in drinks if value_key in d]
        previous_list = state.get("values", [])
        current_value = len(current_list)
        previous_value = state.get("processed_records")

        if (current_value == previous_value) or state["last_run_status"] == "failed":
            context.log.info(
                f"🔁 SKIPPED LOAD for {table_name}:\n"
                f"📅 Previous: {previous_value}\n"
                f"📦 Current: {current_value}\n"
                f"⏳ No new data for {table_name}. Skipping..."
            )
            state["last_run_status"] = "skipped_no_new_data"
            return

        state["processed_records"] = current_value
        state["values"] = current_list
        state["last_run_status"] = "success"

        for drink in drinks:
            yield drink

    return resource_func


@dlt.source
def beverage_source(context: OpExecutionContext):
    TABLE_PARAMS = {
        "beverages": ["c=list", "strCategory"],
        "glasses": ["g=list", "strGlass"],
        "ingredients": ["i=list", "strIngredient1"],
        "alcoholic": ["a=list", "strAlcoholic"]
    }
    for table_name, (param, value_key) in TABLE_PARAMS.items():
        yield create_resource(table_name, param, value_key, context)


@asset(compute_kind="python")
def beverage_dim_data(context) -> dict:
    """Loads beverage dimension tables if row counts differ from expected."""

    context.log.info("Starting DLT pipeline...")
    pipeline = dlt.pipeline(
        pipeline_name="beverages_pipeline",
        destination="duckdb",
        dataset_name="beverage_data",
        dev_mode=False,
    )
    source = beverage_source(context)
    all_values = {}

    for res in source.resources.values():
        try:
            for item in res():
                if isinstance(item, dict):
                    for k, v in item.items():
                        if k not in all_values:
                            all_values[k] = []
                        if v not in all_values[k]:  # optional: avoid duplicates
                            all_values[k].append(v)
                    # break
        except Exception as e:
            context.log.warning(f"Resource failed: {e}")

    # Run the pipeline
    context.log.info("Running pipeline...")
    load_info = pipeline.run(source)
    context.log.info(f"Pipeline finished. Load info: {load_info}")

    return all_values


def create_dimension_resource(table_name, config, values, context):
    @dlt.resource(name=config["resource_name"], write_disposition="replace")
    def resource_func():
        categories = values.get(config["sql_column"], [])

        API_KEY = os.getenv('BEVERAGE_API_KEY')
        if not API_KEY:
            raise ValueError(
                "Environment variable BEVERAGE_API_KEY is not set or empty.")

        state = dlt.current.source_state().setdefault(config["resource_name"], {
            "processed_records": 0,
            "last_run_status": None
        })

        total_records = 0

        for value in categories:
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
        if total_records == previous_value or state["last_run_status"] == "failed":
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
def dimension_data_source(context: OpExecutionContext, values: dict):

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
        yield create_dimension_resource(table_name, config, values, context)


@asset(compute_kind="python", deps=["beverage_dim_data"])
def dimension_data(context, beverage_dim_data: dict) -> bool:

    if not beverage_dim_data:
        context.log.warning(
            "\n⚠️  WARNING: beverage_dim_data SKIPPED\n"
            "📉 No data was loaded from beverage_dim_data.\n"
            "🚫 Skipping dimension_data run.\n"
            "----------------------------------------"
        )
        return False

    context.log.info("Starting DLT pipeline...")
    pipeline = dlt.pipeline(
        pipeline_name="beverage_pipeline",
        destination="duckdb",
        dataset_name="beverage_data",
        dev_mode=False
    )

    source = dimension_data_source(context, beverage_dim_data)
    # run pipeline
    try:
        load_info = pipeline.run(source)
        context.log.info(f"✅ Load successful: {load_info}")
        return bool(load_info)
    except Exception as e:
        context.log.error(f"❌ Pipeline run failed: {e}")
        return False


@asset(compute_kind="python", deps=["dimension_data"])
def beverage_fact_data(context, dimension_data: bool) -> bool:
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

                for drink in drinks:
                    yield drink

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
            destination="duckdb",
            dataset_name="beverage_data",
            dev_mode=False
        )

        load_info = pipeline.run(alcoholic_beverages())
        context.log.info(f"DLT pipeline run complete: {load_info}")
        return bool(load_info)

    except Exception as e:
        context.log.error("DLT pipeline run failed", exc_info=True)
        raise


@asset(deps=["beverage_fact_data"])
def dbt_beverage_data(context: OpExecutionContext, beverage_fact_data: bool):
    """Runs the dbt command after loading the data from Beverage API."""
    if not beverage_fact_data:
        context.log.warning(
            "\n⚠️  WARNING: beverage_fact_data SKIPPED\n"
            "📉 No data was loaded from beverage_fact_data.\n"
            "🚫 Skipping DBT Build.\n"
            "----------------------------------------"
        )
        return False
    DBT_PROJECT_DIR = Path("/workspaces/CamOnDagster/dbt").resolve()
    context.log.info(f"DBT Project Directory: {DBT_PROJECT_DIR}")

    result = subprocess.run(
        "dbt build --select source:beverages+",
        shell=True,
        cwd=DBT_PROJECT_DIR,
        capture_output=True,
        text=True
    )

    context.log.info(result.stdout)
    if result.stderr:
        context.log.error(result.stderr)

    if result.returncode != 0:
        raise Exception(f"dbt build failed with code {result.returncode}")
