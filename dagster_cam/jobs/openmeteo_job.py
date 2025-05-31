from dagster import job
from dagster_cam.assets.open_meteo import openmeteo_asset, dbt_meteo_data


@job(tags={"source": "Open_Meteo"})
def open_meteo_job():
    # First, run the gsheet_finance_data asset
    outcome = openmeteo_asset()

    dbt_meteo_data(outcome)
