from dagster import job
from cam_on_dagster_dbt.assets.beverage_dim_assets import beverage_dim_data
# from cam_on_dagster_dbt.assets.gsheets_customdbt import run_dbt_command


@job
def beverage_dim_job():
    # First, run the gsheet_finance_data asset
    beverage_dim_data()
