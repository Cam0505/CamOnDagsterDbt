from dagster import asset
import os
import requests
from dotenv import load_dotenv
import dlt
import logging
import duckdb

load_dotenv(dotenv_path="/workspaces/CamOnDagster/.env")
logging.basicConfig(level=logging.INFO)


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
    try:
        table_counts = {}
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
    needs_update = False
    for dimension, config in DIMENSION_CONFIG.items():
        current = table_counts.get(dimension, -1)
        if current != config["row_count"]:
            context.log.info(
                f"Table `{dimension}` needs update: expected {config['row_count']}, found {current}")
            needs_update = True

    # Assumes that Dimensions won't change, API is unlikely to change so this is just a check to prevent reloading
    # Would need to check API JSON Length for each table to be more accurate
    if not needs_update:
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
        return [make_resource(dim) for dim in DIMENSION_CONFIG.keys()]

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
