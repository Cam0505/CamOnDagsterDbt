from dagster import job, define_asset_job
from dagster_cam.assets.uv import uv_asset, dbt_uv_data


# @job(tags={"source": "UV"})
# def uv_job():
#     # First, run the uv_asset
#     uv_asset()


uv_job = define_asset_job(
    name="uv_job",
    # Dagster auto-infers dependency on `uv_asset`
    selection=[uv_asset, dbt_uv_data]
)
