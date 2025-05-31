from dagster import job
from dagster_cam.assets.rick_and_morty import rick_and_morty_asset, dbt_rick_and_morty_data


@job(tags={"source": "RickAndMorty"})
def RickandMorty_job():
    # First, run the gsheet_finance_data asset
    outcome = rick_and_morty_asset()

    dbt_rick_and_morty_data(outcome)
