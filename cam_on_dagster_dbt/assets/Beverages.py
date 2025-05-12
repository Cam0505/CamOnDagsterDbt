from dagster import asset, OpExecutionContext
import os
import requests
from pathlib import Path
from dotenv import load_dotenv
import dlt
import duckdb
import time
import subprocess

load_dotenv(dotenv_path="/workspaces/CamOnDagster/.env")


@asset(compute_kind="python")
def beverage_dim_data(context) -> bool:
    """Loads beverage dimension tables if row counts differ from expected."""

    EXPECTED_ROW_COUNTS = {
        "alcoholic": 3,
        "beverages": 11,
        "glasses": 32,
        "ingredients": 489,
    }

    # --- Step 1: Check current row counts in DuckDB ---
    db_path = os.getenv("DESTINATION__DUCKDB__CREDENTIALS")
    if not db_path:
        raise ValueError(
            "Missing DuckDB path in DESTINATION__DUCKDB__CREDENTIALS")

    conn = duckdb.connect(database=db_path)
    table_counts = {}
    try:
        for table, expected_count in EXPECTED_ROW_COUNTS.items():
            try:
                count = conn.execute(
                    f"SELECT COUNT(*) FROM beverage_data.{table}"
                ).fetchone()
                if count is not None:
                    count = count[0]  # Extract the count from the tuple
                else:
                    count = -1  # Handle case where table is empty or doesn't exist
                table_counts[table] = count
            except Exception:
                table_counts[table] = -1  # Table probably doesn't exist
    finally:
        conn.close()

    # --- Step 2: Compare counts to expected ---
    tables_to_update = []
    for table, expected in EXPECTED_ROW_COUNTS.items():
        current = table_counts.get(table, -1)
        if current != expected:
            context.log.info(
                f"Table `{table}` needs update: expected {expected}, found {current}")
            tables_to_update.append(table)

    # Assumes that Dimensions won't change, API is unlikely to change so this is just a check to prevent reloading
    # Would need to check API JSON Length for each table to be more accurate
    if not tables_to_update:
        context.log.info(
            "All Dimension tables are up to date. Skipping DLT pipeline.")
        return True

    # --- Step 3: Define DLT pipeline and load data ---
    TABLE_PARAMS = {
        "beverages": "c=list",
        "glasses": "g=list",
        "ingredients": "i=list",
        "alcoholic": "a=list"
    }

    def make_resource(table_name):
        param = TABLE_PARAMS[table_name]

        @dlt.resource(name=table_name, write_disposition="replace")
        def resource_func():
            url = f"https://www.thecocktaildb.com/api/json/v2/{os.getenv('BEVERAGE_API_KEY')}/list.php?{param}"
            response = requests.get(url)
            response.raise_for_status()
            drinks = response.json()["drinks"]
            yield drinks
        return resource_func

    @dlt.source
    def beverages():
        return [make_resource(table)() for table in tables_to_update]

    pipeline = dlt.pipeline(
        pipeline_name="beverages_pipeline",
        destination="duckdb",
        dataset_name="beverage_data",
        dev_mode=False,
    )

    try:
        load_info = pipeline.run(beverages())
        context.log.info(f"DLT pipeline load successful: {load_info}")
        return True
    except Exception as e:
        context.log.error(f"Pipeline run failed: {e}")
        return False


@asset(compute_kind="python", deps=["beverage_dim_data"])
def dimension_data(context) -> bool:

    DIMENSION_CONFIG = {
        "ingredients": {
            "sql_column": "str_ingredient1",
            "query_param": "i",
            "source_key": "source_ingredient",
            "resource_name": "ingredients_table",
            "row_count": 2473
        },
        "alcoholic": {
            "sql_column": "str_alcoholic",
            "query_param": "a",
            "source_key": "source_alcohol_type",
            "resource_name": "alcoholic_table",
            "row_count": 636
        },
        "beverages": {
            "sql_column": "str_category",
            "query_param": "c",
            "source_key": "source_beverage_type",
            "resource_name": "beverages_table",
            "row_count": 636
        },
        "glasses": {
            "sql_column": "str_glass",
            "query_param": "g",
            "source_key": "source_glass",
            "resource_name": "glass_table",
            "row_count": 636
        }
    }
    # --- Step 1: Check current row counts in DuckDB ---
    db_path = os.getenv("DESTINATION__DUCKDB__CREDENTIALS")
    if not db_path:
        raise ValueError(
            "Missing DuckDB path in DESTINATION__DUCKDB__CREDENTIALS")
    conn = duckdb.connect(database=db_path)
    table_counts = {}
    try:
        for dimension, config in DIMENSION_CONFIG.items():
            try:
                count = conn.execute(
                    f"SELECT COUNT(*) FROM beverage_data.{config['resource_name']}"
                ).fetchone()
                if count is not None:
                    count = count[0]  # Extract the count from the tuple
                else:
                    count = -1  # Handle case where table is empty or doesn't exist
                table_counts[dimension] = count
            except Exception:
                table_counts[dimension] = -1  # Table probably doesn't exist
    finally:
        conn.close()

    # --- Step 2: Compare counts to expected ---
    tables_to_update = []
    for dimension, config in DIMENSION_CONFIG.items():
        current = table_counts.get(dimension, -1)
        if current != config["row_count"]:
            context.log.info(
                f"Table `{dimension}` needs update: expected {config['row_count']}, found {current}")
            tables_to_update.append(dimension)

    # Assumes that Dimensions won't change, API is unlikely to change so this is just a check to prevent reloading
    # Would need to check API JSON Length for each table to be more accurate
    if not tables_to_update:
        context.log.info(
            "All Dimension Data tables are up to date. Skipping DLT pipeline.")
        return True

    def get_dim(table_name: str, column_name: str) -> list[str]:

        conn = duckdb.connect(database=db_path)
        rows = conn.execute(
            f"SELECT DISTINCT {column_name} FROM beverage_data.{table_name}").fetchall()
        conn.close()
        # Extract the values from tuples
        return [row[0] for row in rows]

    def make_resource(dimension: str):
        config = DIMENSION_CONFIG[dimension]

        @dlt.resource(name=config["resource_name"], write_disposition="replace")
        def resource_func():
            values = get_dim(dimension, config["sql_column"])
            API_KEY = os.getenv('BEVERAGE_API_KEY')
            if not API_KEY:
                raise ValueError(
                    "Environment variable BEVERAGE_API_KEY is not set or empty.")
            for value in values:
                url = f"https://www.thecocktaildb.com/api/json/v2/{API_KEY}/filter.php?{config['query_param']}={value}"
                response = requests.get(url)
                response.raise_for_status()  # Raise exception on error
                drinks = response.json()["drinks"]
                for drink in drinks:
                    if isinstance(drink, dict):  # Ensure it's a dictionary
                        drink[config["source_key"]] = value
                        yield drink

        return resource_func()

    @dlt.source
    def beverage_filter_data():
        return [make_resource(dim) for dim in tables_to_update]

    pipeline = dlt.pipeline(
        pipeline_name="beverage_pipeline",
        destination="duckdb",
        dataset_name="beverage_data",
        dev_mode=False
    )

    # load info
    load_info = None
    # run pipeline
    try:
        load_info = pipeline.run(beverage_filter_data())
        context.log.info(f"Load successful: {load_info}")
    except Exception as e:
        context.log.error(f"Pipeline run failed: {e}")

    # Check if load_info is not None and contains data
    return bool(load_info)


@asset(compute_kind="python", deps=["dimension_data"])
def beverage_fact_data(context) -> bool:

    @dlt.resource(name="consumption", write_disposition="append")
    def beverages_api():
        API_KEY = os.getenv("BEVERAGE_API_KEY")
        if not API_KEY:
            raise ValueError(
                "Environment variable BEVERAGE_API_KEY is not set or empty.")

        url = f"https://www.thecocktaildb.com/api/json/v2/{API_KEY}/randomselection.php"

        for i in range(50):
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
def dbt_beverage_data(context: OpExecutionContext):
    """Runs the dbt command after loading the data from Beverage API."""
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
