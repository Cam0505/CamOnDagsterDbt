
# from dagster_cam.jobs import gsheets_financial_with_dbt_job
# from dagster_cam.assets import openmeteo_asset, dbt_meteo_data
# from dagster_cam.jobs import open_meteo_job
from dagster_cam.jobs import openlibrary_job
from dagster_cam.assets import openlibrary_books_asset, openlibrary_subjects_asset, dbt_openlibrary_data
from dagster_cam.sensors import camon_sensor
from dagster_cam.schedules import schedules
# from dagster_cam.assets import gsheet_finance_data, gsheet_dbt_command
from dotenv import load_dotenv
import os
from dagster_duckdb_pandas import DuckDBPandasIOManager
from dagster import Definitions, define_asset_job
from dagster_dbt import DbtCliResource

from dagster_cam.assets import DBT_PROJECT_DIR

MotherDuck = os.getenv("MD")
if not MotherDuck:
    raise ValueError(
        "Environment variable 'MD' is not set. Set it to your MotherDuck connection string (e.g., md:?token=...).")
# definitions.py

# Import Rick and Morty assets and jobs (active)
# from dagster_cam.assets import rick_and_morty_asset, dbt_rick_and_morty_data
# from dagster_cam.jobs import RickandMorty_job

# Uncomment and import other assets/jobs as needed:

# from dagster_cam.assets import camon_dbt_assets


# from dagster_cam.jobs import airline_job
# from dagster_cam.assets import openflights_data

# Beverages Assets
# from dagster_cam.assets import dimension_data, beverage_fact_data, dbt_beverage_data

# Meals Assets
# from dagster_cam.assets import meals_dim_data, meals_dimension_data, meals_fact_data, dbt_meals_data

# OpenLibrary Assets

# GeoAPI Assets
# from dagster_cam.assets import get_geo_data, dbt_geo_data

# from dagster_cam.assets import uv_asset
# from dagster_cam.jobs import uv_job

# Jobs - uncomment as needed
# from dagster_cam.jobs import beverage_dim_job
# from dagster_cam.jobs import meals_dim_job
# from dagster_cam.jobs import geo_data_job

# Youtube
# from dagster_cam.assets.youtube import youtube_pipeline
# from dagster_cam.jobs import Youtube_Job


# Define the assets
all_assets = [openlibrary_books_asset,
              openlibrary_subjects_asset, dbt_openlibrary_data]

# Register the job, sensor, and schedule in the Definitions
defs = Definitions(
    assets=all_assets,
    # Register only the gsheets job
    jobs=[openlibrary_job],
    schedules=[schedules]  # ,
    # sensors=[camon_sensor]
    , resources={
        "io_manager": DuckDBPandasIOManager(database=MotherDuck),
        "dbt": DbtCliResource(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROJECT_DIR),
    }
)

# Execute the job immediately
if __name__ == "__main__":
    try:
        result = openlibrary_job.execute_in_process(
            resources={
                "dbt": DbtCliResource(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROJECT_DIR),
            }
        )
        print("openlibrary_job Job finished:", result)
    except Exception as e:
        print(f"Error executing job: {e}")
