from dagster import job
from cam_on_dagster_dbt.assets.beverage_dim_assets import beverage_dim_data
from cam_on_dagster_dbt.assets.beverage_dim_data_assets import dimension_data
from cam_on_dagster_dbt.assets.beverage_fact_assets import beverage_fact_data
from cam_on_dagster_dbt.assets.beverage_dbtBuild import dbt_beverage_data


@job
def beverage_dim_job():
    # First, run the gsheet_finance_data asset
    beverage_dim_values = beverage_dim_data()

    # Then, run the dim data asset
    Dimension_data_success = dimension_data(beverage_dim_values)

    Fact_data_success = beverage_fact_data(Dimension_data_success)

    dbt_beverage_data(Fact_data_success)
