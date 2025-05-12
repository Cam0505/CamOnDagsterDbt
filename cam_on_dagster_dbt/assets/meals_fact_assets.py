from dagster import asset
import os
import requests
from dotenv import load_dotenv
import dlt
import time

load_dotenv(dotenv_path="/workspaces/CamOnDagster/.env")


@asset(compute_kind="python", deps=["meals_dimensions"])
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
