from dagster import asset
import os
import requests
from dotenv import load_dotenv
import dlt
import logging
import duckdb

load_dotenv(dotenv_path="/workspaces/CamOnDagster/.env")
logging.basicConfig(level=logging.INFO)

# Expected row counts for each dimension
EXPECTED_ROW_COUNTS = {
    "alcoholic": 3,
    "beverages": 11,
    "glasses": 32,
    "ingredients": 489,
}


@asset(compute_kind="python")
def beverage_dim_data(context) -> bool:
    """Loads beverage dimension tables if row counts differ from expected."""

    # --- Step 1: Check current row counts in DuckDB ---
    db_path = os.getenv("DESTINATION__DUCKDB__CREDENTIALS")
    if not db_path:
        raise ValueError(
            "Missing DuckDB path in DESTINATION__DUCKDB__CREDENTIALS")

    conn = duckdb.connect(database=db_path)
    try:
        table_counts = {}
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
    needs_update = False
    for table, expected in EXPECTED_ROW_COUNTS.items():
        current = table_counts.get(table, -1)
        if current != expected:
            context.log.info(
                f"Table `{table}` needs update: expected {expected}, found {current}")
            needs_update = True

    # Assumes that Dimensions won't change, API is unlikely to change so this is just a check to prevent reloading
    # Would need to check API JSON Length for each table to be more accurate
    if not needs_update:
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

    def make_resource(table_name, param):
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
        return [make_resource(table, param)() for table, param in TABLE_PARAMS.items()]

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
