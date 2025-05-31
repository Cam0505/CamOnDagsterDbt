from dagster import job
from dagster_cam.assets.dbt_assets import camon_dbt_assets


# Define job for DBT assets
@job
def run_dbt_assets():
    camon_dbt_assets()
