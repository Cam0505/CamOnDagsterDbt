"""
To add a daily schedule that materializes your dbt assets, uncomment the following lines.
"""
from dagster import ScheduleDefinition
from dagster_cam.jobs.dbt_job import run_dbt_assets
from dagster_cam.jobs.gsheets_job import gsheets_financial_with_dbt_job
from dagster_cam.jobs.beverage_data_job import beverage_dim_job
from dagster_cam.jobs.meals_data_job import meals_dim_job


schedules = ScheduleDefinition(
    job=run_dbt_assets,
    cron_schedule="0 6 * * *",  # Daily at 6 AM
)

# At minute 10 past every hour from 9 through 17.
Hourly_GSHEET_SCHEDULE = ScheduleDefinition(
    job=gsheets_financial_with_dbt_job,
    cron_schedule="10 9-17 * * *"
)

# At 10:00 on Monday.
Weekly_BEVERAGE_SCHEDULE = ScheduleDefinition(
    job=beverage_dim_job,
    cron_schedule="0 10 * * MON"
)

# At 10:00 on Wednesday.
Weekly_MEALS_SCHEDULE = ScheduleDefinition(
    job=meals_dim_job,
    cron_schedule="0 10 * * WED"
)
