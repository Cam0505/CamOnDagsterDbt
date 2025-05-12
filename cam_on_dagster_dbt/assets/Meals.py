from dagster import asset, OpExecutionContext
import os
import requests
from dotenv import load_dotenv
from pathlib import Path
import dlt
import duckdb
import time
import subprocess


load_dotenv(dotenv_path="/workspaces/CamOnDagster/.env")


@asset(compute_kind="python")
def meals_dim_data(context) -> bool:
    """Loads beverage dimension tables if row counts differ from expected."""

    EXPECTED_ROW_COUNTS = {
        "category": 14,
        "country": 29,
        "ingredients": 575
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
                    f"SELECT COUNT(*) FROM meals_data.{table}"
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
        "category": "c=list",
        "country": "a=list",
        "ingredients": "i=list"
    }

    def make_resource(table_name):
        param = TABLE_PARAMS[table_name]

        @dlt.resource(name=table_name, write_disposition="replace")
        def resource_func():
            url = f"https://www.themealdb.com/api/json/v1/1/list.php?{param}"
            context.log.info(f"Fetching data for {table_name} from {url}")
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                data = response.json()
                if "meals" not in data or data["meals"] is None:
                    context.log.warning(
                        f"No 'meals' found in API response for {table_name}")
                    return
                yield data["meals"]
                context.log.info(f"Fetched and yielded data for {table_name}")
            except requests.exceptions.RequestException as e:
                context.log.error(
                    f"Request failed for {table_name}: {e}", exc_info=True)
                raise
            except ValueError as e:
                context.log.error(
                    f"JSON decode failed for {table_name}: {e}", exc_info=True)
                raise
        return resource_func

    @dlt.source
    def meals():
        resources = []
        for table in tables_to_update:
            try:
                resource = make_resource(table)
                resources.append(resource())
            except Exception as e:
                context.log.error(
                    f"Failed to create resource for {table}: {e}", exc_info=True)
        return resources

    pipeline = dlt.pipeline(
        pipeline_name="meals_pipeline",
        destination="duckdb",
        dataset_name="meals_data",
        dev_mode=False,
    )

    try:
        load_info = pipeline.run(meals())
        context.log.info(f"DLT pipeline load successful: {load_info}")
        return True
    except Exception as e:
        context.log.error(f"Pipeline run failed: {e}")
        return False


@asset(compute_kind="python", deps=["meals_dim_data"])
def meals_dimension_data(context) -> bool:

    DIMENSION_CONFIG = {
        "ingredients": {
            "sql_column": "str_ingredient",
            "query_param": "i",
            "source_key": "source_ingredient",
            "resource_name": "ingredient_table",
            "row_count": 2781
        },
        "country": {
            "sql_column": "str_area",
            "query_param": "a",
            "source_key": "source_country",
            "resource_name": "country_table",
            "row_count": 304
        },
        "category": {
            "sql_column": "str_category",
            "query_param": "c",
            "source_key": "source_category",
            "resource_name": "category_table",
            "row_count": 304
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
                    f"SELECT COUNT(*) FROM meals_data.{config['resource_name']}"
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
            f"SELECT DISTINCT {column_name} FROM meals_data.{table_name}").fetchall()
        conn.close()
        # Extract the values from tuples
        return [row[0] for row in rows]

    def make_resource(dimension: str):
        config = DIMENSION_CONFIG[dimension]

        @dlt.resource(name=config["resource_name"], write_disposition="replace")
        def resource_func():
            values = get_dim(dimension, config["sql_column"])

            for value in values:
                if not value:
                    continue
                url = f"https://www.themealdb.com/api/json/v1/1/filter.php?{config['query_param']}={value}"
                try:
                    response = requests.get(url, timeout=15)
                    response.raise_for_status()  # Raise exception on error
                    meals = response.json().get("meals")
                    if meals is None:
                        context.log.warning(
                            f"No meals found for {dimension}='{value}'")
                        continue
                    for meal in meals:
                        if isinstance(meal, dict):  # Ensure it's a dictionary
                            meal[config["source_key"]] = value
                            yield meal
                        else:
                            context.log.warning(
                                f"Invalid meal format for {value}: {meal}")
                except requests.RequestException as e:
                    context.log.error(f"HTTP error for {value} ({url}): {e}")
                except Exception as e:
                    context.log.error(f"Unexpected error for {value}: {e}")

        return resource_func()

    @dlt.source
    def beverage_filter_data():
        return [make_resource(dim) for dim in tables_to_update]

    pipeline = dlt.pipeline(
        pipeline_name="meals_pipeline",
        destination="duckdb",
        dataset_name="meals_data",
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


@asset(compute_kind="python", deps=["meals_dimension_data"])
def meals_fact_data(context) -> bool:

    @dlt.resource(name="consumption", write_disposition="append")
    def meals_api():

        url = f"https://www.themealdb.com/api/json/v1/1/random.php"
        all_meals = []

        for i in range(200):
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                meals = response.json().get("meals")
                if not meals:
                    context.log.warning(
                        f"No meals returned in iteration {i+1}")
                    continue
                for meal in meals:
                    yield meal

                time.sleep(0.2)  # Prevent throttling
            except requests.RequestException as e:
                context.log.error(
                    f"Request failed on iteration {i+1}: {e}", exc_info=True)
            except Exception as e:
                context.log.error(
                    f"Unexpected error on iteration {i+1}: {e}", exc_info=True)

    @dlt.source
    def meals():
        return meals_api()

    try:
        pipeline = dlt.pipeline(
            pipeline_name="meals_pipeline",
            destination="duckdb",
            dataset_name="meals_data",
            dev_mode=False
        )

        load_info = pipeline.run(meals())
        context.log.info(f"DLT pipeline run complete: {load_info}")
        return bool(load_info)

    except Exception as e:
        context.log.error("DLT pipeline run failed", exc_info=True)
        raise
    finally:
        # Ensure that the connection is closed even if an error occurs
        return True


@asset(deps=["meals_fact_data"])
def dbt_meals_data(context: OpExecutionContext):
    """Runs the dbt command after loading the data from Beverage API."""
    DBT_PROJECT_DIR = Path("/workspaces/CamOnDagster/dbt").resolve()
    context.log.info(f"DBT Project Directory: {DBT_PROJECT_DIR}")

    result = subprocess.run(
        "dbt build --select source:meals+",
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
