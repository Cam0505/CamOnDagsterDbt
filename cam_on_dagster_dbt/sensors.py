from dagster import RunRequest, SensorDefinition
from .jobs.jobs import cams_job

camon_sensor = SensorDefinition(
    name="cam_sensor",
    evaluation_fn=lambda _: [RunRequest(run_key=None, job=cams_job)],
    minimum_interval_seconds=60 * 5,
)
