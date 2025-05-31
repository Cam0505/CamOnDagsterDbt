from dagster import job
from dagster_cam.assets.airlines import openflights_data


@job
def airline_job():
    openflights_data()
