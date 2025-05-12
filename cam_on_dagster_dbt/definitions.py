from dagster import Definitions, define_asset_job
# from cam_on_dagster_dbt.assets.dbt_assets import camon_dbt_assets
# from cam_on_dagster_dbt.assets.gsheets_assets import gsheet_finance_data
# from cam_on_dagster_dbt.assets.gsheets_customdbt import run_dbt_command
# from cam_on_dagster_dbt.assets.beverage_dim_assets import beverage_dim_data
# from cam_on_dagster_dbt.assets.beverage_dim_data_assets import dimension_data
# from cam_on_dagster_dbt.assets.beverage_fact_assets import beverage_fact_data
# from cam_on_dagster_dbt.assets.beverage_dbtBuild import dbt_beverage_data
# MEALS Assets
from cam_on_dagster_dbt.assets.meals_dim_assets import meals_dim_data
from cam_on_dagster_dbt.assets.meals_dim_data_assets import meals_dimension_data
from cam_on_dagster_dbt.assets.meals_fact_assets import meals_fact_data
from cam_on_dagster_dbt.assets.meals_dbtBuild import dbt_meals_data
# Import only the necessary job
# from cam_on_dagster_dbt.jobs.gsheets_job import gsheets_financial_with_dbt_job
# from cam_on_dagster_dbt.jobs.beverage_data_job import beverage_dim_job
from cam_on_dagster_dbt.jobs.meals_data_job import meals_dim_job

from cam_on_dagster_dbt.sensors import camon_sensor
from cam_on_dagster_dbt.schedules import schedules

# Define the assets
all_assets = [meals_dim_data, meals_dimension_data,
              meals_fact_data, dbt_meals_data]

# Register the job, sensor, and schedule in the Definitions
defs = Definitions(
    assets=all_assets,
    # Register only the gsheets job
    jobs=[meals_dim_job],
    schedules=[schedules]  # ,
    # sensors=[camon_sensor]
)

# Execute the job immediately
if __name__ == "__main__":
    # Run the gsheets_financial_with_dbt_job once immediately
    # result = gsheets_financial_with_dbt_job.execute_in_process()
    try:
        result = meals_dim_job.execute_in_process()
        print("Beverage Job finished:", result.success)
    except Exception as e:
        print(f"Error executing job: {e}")
