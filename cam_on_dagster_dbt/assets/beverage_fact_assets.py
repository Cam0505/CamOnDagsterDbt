from dagster import asset
import os
import requests
from dotenv import load_dotenv
import dlt
import time

load_dotenv(dotenv_path="/workspaces/CamOnDagster/.env")


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
