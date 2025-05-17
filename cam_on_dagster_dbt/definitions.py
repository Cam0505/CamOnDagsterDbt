# definitions.py
from dagster import Definitions, define_asset_job

# Import Rick and Morty assets and jobs (active)
# from cam_on_dagster_dbt.assets import rick_and_morty_asset, dbt_rick_and_morty_data
# from cam_on_dagster_dbt.jobs import RickandMorty_job

# Uncomment and import other assets/jobs as needed:

# from cam_on_dagster_dbt.assets import camon_dbt_assets
from cam_on_dagster_dbt.assets import gsheet_finance_data, gsheet_dbt_command

# Beverages Assets
# from cam_on_dagster_dbt.assets import beverage_dim_data, dimension_data, beverage_fact_data, dbt_beverage_data

# Meals Assets
# from cam_on_dagster_dbt.assets import meals_dim_data, meals_dimension_data, meals_fact_data, dbt_meals_data

# OpenLibrary Assets
# from cam_on_dagster_dbt.assets import openlibrary_books_asset, dbt_openlibrary_data

# GeoAPI Assets
# from cam_on_dagster_dbt.assets import get_geo_data, dbt_geo_data

# Jobs - uncomment as needed
from cam_on_dagster_dbt.jobs import gsheets_financial_with_dbt_job
# from cam_on_dagster_dbt.jobs import beverage_dim_job
# from cam_on_dagster_dbt.jobs import meals_dim_job
# from cam_on_dagster_dbt.jobs import geo_data_job
# from cam_on_dagster_dbt.jobs import openlibrary_job

from cam_on_dagster_dbt.sensors import camon_sensor
from cam_on_dagster_dbt.schedules import schedules

# Define the assets
all_assets = [gsheet_finance_data, gsheet_dbt_command]

# Register the job, sensor, and schedule in the Definitions
defs = Definitions(
    assets=all_assets,
    # Register only the gsheets job
    jobs=[gsheets_financial_with_dbt_job],
    schedules=[schedules]  # ,
    # sensors=[camon_sensor]
)

# Execute the job immediately
if __name__ == "__main__":
    try:
        result = gsheets_financial_with_dbt_job.execute_in_process()
        print("gsheets_financial_with_dbt_job Job finished:", result.success)
    except Exception as e:
        print(f"Error executing job: {e}")
