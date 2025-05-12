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
    "category": 14,
    "country": 29,
    "ingredients": 575
}


@asset(compute_kind="python")
def meals_dim_data(context) -> bool:
    """Loads beverage dimension tables if row counts differ from expected."""

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
            logging.info(f"Fetching data for {table_name} from {url}")
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                data = response.json()
                if "meals" not in data or data["meals"] is None:
                    logging.warning(
                        f"No 'meals' found in API response for {table_name}")
                    return
                yield data["meals"]
                logging.info(f"Fetched and yielded data for {table_name}")
            except requests.exceptions.RequestException as e:
                logging.error(
                    f"Request failed for {table_name}: {e}", exc_info=True)
                raise
            except ValueError as e:
                logging.error(
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
                logging.error(
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
