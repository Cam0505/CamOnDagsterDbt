from dagster import job, define_asset_job
from dagster_cam.assets.open_meteo import openmeteo_asset, dbt_meteo_data


# @job(tags={"source": "Open_Meteo"})
# def open_meteo_job():
#     # First, run the gsheet_finance_data asset
#     outcome = openmeteo_asset()

#     dbt_meteo_data(outcome)


open_meteo_job = define_asset_job(
    name="open_meteo_job",
    # Dagster auto-infers dependency on `openmeteo_asset`
    selection=[openmeteo_asset, dbt_meteo_data]
)
