from dagster import job
from cam_on_dagster_dbt.assets.GeoAPI import get_geo_data, dbt_geo_data


@job
def geo_data_job():
    # First, run the gsheet_finance_data asset
    Geo_Data_Present = get_geo_data()

    # Then, run the dim data asset
    dbt_geo_data(Geo_Data_Present)
