from dagster import job
from cam_on_dagster_dbt.assets.openlibrary import openlibrary_books_asset, dbt_openlibrary_data


@job
def openfood_job():
    # First, run the gsheet_finance_data asset
    outcome = openlibrary_books_asset()

    dbt_openlibrary_data(outcome)
