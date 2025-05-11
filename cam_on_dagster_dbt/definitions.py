from dagster import Definitions
from .assets.dbt_assets import camon_dbt_assets
from .jobs.jobs import cams_job
from .sensors import camon_sensor
from .schedules import schedules

all_assets = [*camon_dbt_assets]

defs = Definitions(
    assets=all_assets,
    jobs=[cams_job],
    schedules=[schedules],
    sensors=[camon_sensor]
)
