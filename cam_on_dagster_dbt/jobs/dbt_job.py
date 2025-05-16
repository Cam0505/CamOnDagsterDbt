from dagster import job
from cam_on_dagster_dbt.assets.dbt_assets import camon_dbt_assets


# Define job for DBT assets
@job
def run_dbt_assets():
    camon_dbt_assets()
