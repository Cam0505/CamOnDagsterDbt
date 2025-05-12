from dagster import job
from cam_on_dagster_dbt.assets.meals_dim_assets import meals_dim_data
from cam_on_dagster_dbt.assets.meals_dim_data_assets import meals_dimension_data
from cam_on_dagster_dbt.assets.meals_fact_assets import meals_fact_data
from cam_on_dagster_dbt.assets.meals_dbtBuild import dbt_meals_data


@job
def meals_dim_job():
    # First, run the gsheet_finance_data asset
    categories_loaded = meals_dim_data()

    # Then, run the dim data asset
    category_ids_loaded = meals_dimension_data(categories_loaded)

    Fact_data_success = meals_fact_data(category_ids_loaded)

    dbt_meals_data(Fact_data_success)
