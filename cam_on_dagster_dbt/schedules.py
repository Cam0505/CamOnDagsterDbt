"""
To add a daily schedule that materializes your dbt assets, uncomment the following lines.
"""
from dagster import ScheduleDefinition
from .jobs.jobs import run_dbt_assets


schedules = ScheduleDefinition(
    job=run_dbt_assets,
    cron_schedule="0 6 * * *",  # Daily at 6 AM
)
