from dagster import asset
import os
import requests
from dotenv import load_dotenv
import dlt
import logging
import duckdb

load_dotenv(dotenv_path="/workspaces/CamOnDagster/.env")
logging.basicConfig(level=logging.INFO)


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
                        logging.warning(
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
