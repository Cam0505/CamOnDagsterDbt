from dagster import Definitions
# from cam_on_dagster_dbt.assets.dbt_assets import camon_dbt_assets
from cam_on_dagster_dbt.assets.gsheets_assets import gsheet_finance_data
from cam_on_dagster_dbt.assets.gsheets_customdbt import run_dbt_command
# Import only the necessary job
from cam_on_dagster_dbt.jobs.gsheets_job import gsheets_financial_with_dbt_job
from cam_on_dagster_dbt.sensors import camon_sensor
from cam_on_dagster_dbt.schedules import schedules

# Define the assets
all_assets = [gsheet_finance_data, run_dbt_command]

# Register the job, sensor, and schedule in the Definitions
defs = Definitions(
    assets=all_assets,
    jobs=[gsheets_financial_with_dbt_job],  # Register only the gsheets job
    schedules=[schedules]  # ,
    # sensors=[camon_sensor]
)

# Execute the job immediately
if __name__ == "__main__":
    # Run the gsheets_financial_with_dbt_job once immediately
    result = gsheets_financial_with_dbt_job.execute_in_process()
    print("Job finished:", result.success)
