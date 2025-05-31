from dagster import define_asset_job
from dagster_cam.assets.Gsheets import gsheet_finance_data, dbt_models


gsheets_financial_with_dbt_job = define_asset_job(
    name="gsheets_financial_with_dbt_job",
    # Dagster auto-infers dependency on `gsheet_finance_data`
    selection=[gsheet_finance_data, dbt_models]
)
