"""
To add a daily schedule that materializes your dbt assets, uncomment the following lines.
"""
from dagster import ScheduleDefinition
from .jobs.jobs import cams_job


schedules = ScheduleDefinition(
    job=cams_job,
    cron_schedule="0 6 * * *",  # Daily at 6 AM
)
