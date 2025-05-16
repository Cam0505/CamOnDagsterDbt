from dagster import job
from cam_on_dagster_dbt.assets.Gsheets import gsheet_finance_data, gsheet_dbt_command


@job(tags={"source": "gSheets"})
def gsheets_financial_with_dbt_job():
    # First, run the gsheet_finance_data asset
    gsheet_data = gsheet_finance_data()

    # Then, run the dbt command asset after gsheet_finance_data finishes
    gsheet_dbt_command(gsheet_data)
