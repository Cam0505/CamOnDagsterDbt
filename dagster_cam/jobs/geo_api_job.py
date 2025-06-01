from dagster import job, define_asset_job
from dagster_cam.assets.GeoAPI import get_geo_data, dbt_geo_data


# @job(tags={"source": "Geo"})
# def geo_data_job():
#     # First, run the gsheet_finance_data asset
#     Geo_Data_Present = get_geo_data()

#     # Then, run the dim data asset
#     dbt_geo_data(Geo_Data_Present)

geo_data_job = define_asset_job(
    name="geo_data_job",
    # Dagster auto-infers dependency on `geo_data`
    selection=[get_geo_data, dbt_geo_data]
)
