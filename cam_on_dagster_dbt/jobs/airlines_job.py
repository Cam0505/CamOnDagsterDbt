from dagster import job
from cam_on_dagster_dbt.assets.airlines import openflights_data


@job
def airline_job():
    openflights_data()
