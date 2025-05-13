from dagster import job
from cam_on_dagster_dbt.assets.dbt_assets import camon_dbt_assets
from cam_on_dagster_dbt.assets.Gsheets import gsheet_finance_data, gsheet_dbt_command
from cam_on_dagster_dbt.jobs.gsheets_job import gsheets_financial_with_dbt_job


# Define job for DBT assets
@job
def run_dbt_assets():
    camon_dbt_assets()
