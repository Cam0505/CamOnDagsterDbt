from dagster import job
from dagster_cam.assets.openlibrary import openlibrary_books_asset, openlibrary_subjects_asset, dbt_openlibrary_data


@job(tags={"source": "OpenLibrary"})
def openlibrary_job():
    # First, run the gsheet_finance_data asset
    outcome = openlibrary_books_asset()

    subject_outcome = openlibrary_subjects_asset(outcome)

    dbt_openlibrary_data(subject_outcome)
