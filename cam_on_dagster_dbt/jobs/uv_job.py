from dagster import job
from cam_on_dagster_dbt.assets.uv import uv_asset


@job(tags={"source": "UV"})
def uv_job():
    # First, run the uv_asset
    uv_asset()
