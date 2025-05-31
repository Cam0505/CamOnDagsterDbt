from dagster import job
from dagster_cam.assets.uv import uv_asset


@job(tags={"source": "UV"})
def uv_job():
    # First, run the uv_asset
    uv_asset()
