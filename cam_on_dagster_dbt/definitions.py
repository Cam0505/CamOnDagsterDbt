
# from cam_on_dagster_dbt.jobs import gsheets_financial_with_dbt_job
from cam_on_dagster_dbt.assets import openmeteo_asset, dbt_meteo_data
from cam_on_dagster_dbt.jobs import open_meteo_job
from cam_on_dagster_dbt.sensors import camon_sensor
from cam_on_dagster_dbt.schedules import schedules
# from cam_on_dagster_dbt.assets import gsheet_finance_data, gsheet_dbt_command
from dotenv import load_dotenv
import os
from dagster_duckdb_pandas import DuckDBPandasIOManager
from dagster import Definitions, define_asset_job
from dagster_dbt import DbtCliResource

from cam_on_dagster_dbt.assets import DBT_PROJECT_DIR

MotherDuck = os.getenv("MD")
if not MotherDuck:
    raise ValueError(
        "Environment variable 'MD' is not set. Set it to your MotherDuck connection string (e.g., md:?token=...).")
# definitions.py

# Import Rick and Morty assets and jobs (active)
# from cam_on_dagster_dbt.assets import rick_and_morty_asset, dbt_rick_and_morty_data
# from cam_on_dagster_dbt.jobs import RickandMorty_job

# Uncomment and import other assets/jobs as needed:

# from cam_on_dagster_dbt.assets import camon_dbt_assets


# from cam_on_dagster_dbt.jobs import airline_job
# from cam_on_dagster_dbt.assets import openflights_data

# Beverages Assets
# from cam_on_dagster_dbt.assets import dimension_data, beverage_fact_data, dbt_beverage_data

# Meals Assets
# from cam_on_dagster_dbt.assets import meals_dim_data, meals_dimension_data, meals_fact_data, dbt_meals_data

# OpenLibrary Assets
# from cam_on_dagster_dbt.assets import openlibrary_books_asset, dbt_openlibrary_data

# GeoAPI Assets
# from cam_on_dagster_dbt.assets import get_geo_data, dbt_geo_data

# from cam_on_dagster_dbt.assets import uv_asset
# from cam_on_dagster_dbt.jobs import uv_job

# Jobs - uncomment as needed
# from cam_on_dagster_dbt.jobs import beverage_dim_job
# from cam_on_dagster_dbt.jobs import meals_dim_job
# from cam_on_dagster_dbt.jobs import geo_data_job
# from cam_on_dagster_dbt.jobs import openlibrary_job

# Youtube
# from cam_on_dagster_dbt.assets.youtube import youtube_pipeline
# from cam_on_dagster_dbt.jobs import Youtube_Job


# Define the assets
all_assets = [openmeteo_asset, dbt_meteo_data]

# Register the job, sensor, and schedule in the Definitions
defs = Definitions(
    assets=all_assets,
    # Register only the gsheets job
    jobs=[open_meteo_job],
    schedules=[schedules]  # ,
    # sensors=[camon_sensor]
    , resources={
        "io_manager": DuckDBPandasIOManager(database=MotherDuck),
        "dbt": DbtCliResource(project_dir=DBT_PROJECT_DIR),
    }
)

# Execute the job immediately
if __name__ == "__main__":
    try:
        result = open_meteo_job.execute_in_process()
        print("open_meteo_job Job finished:", result)
    except Exception as e:
        print(f"Error executing job: {e}")
