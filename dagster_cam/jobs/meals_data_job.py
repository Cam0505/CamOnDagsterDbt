from dagster import job
from dagster_cam.assets.Meals import meals_dim_data, meals_dimension_data, meals_fact_data, dbt_meals_data


@job(tags={"source": "Meals"})
def meals_dim_job():
    # First, run the gsheet_finance_data asset
    categories_loaded = meals_dim_data()

    # Then, run the dim data asset
    category_ids_loaded = meals_dimension_data(categories_loaded)

    Fact_data_success = meals_fact_data(category_ids_loaded)

    dbt_meals_data(Fact_data_success)
