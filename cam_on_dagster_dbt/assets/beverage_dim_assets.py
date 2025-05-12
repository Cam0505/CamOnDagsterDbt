from dagster import asset
import os
import requests
from dotenv import load_dotenv
import dlt
import logging

load_dotenv(dotenv_path="/workspaces/CamOnDagster/.env")
logging.basicConfig(level=logging.INFO)


@asset(compute_kind="python")
def beverage_dim_data(context) -> bool:
    # Map table names to API parameters
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
            response.raise_for_status()  # Raise exception on error
            drinks = response.json()["drinks"]
            yield drinks
        return resource_func

    @dlt.source
    def beverages():
        return [make_resource(table, param)() for table, param in TABLE_PARAMS.items()]

    # pipeline
    pipeline = dlt.pipeline(
        pipeline_name="beverages_pipeline",
        destination="duckdb",
        dataset_name="beverage_data",
        dev_mode=False
    )

    context.log.info(f"Writing to DuckDB at: {pipeline.pipeline_name}.duckdb")

    # load info
    load_info = None
    # run pipeline
    try:
        load_info = pipeline.run(beverages())
        context.log.info(f"Load successful: {load_info}")
    except Exception as e:
        context.log.error(f"Pipeline run failed: {e}")

    # Check if load_info is not None and contains data
    return bool(load_info)
