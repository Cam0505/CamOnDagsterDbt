from dagster import job
from cam_on_dagster_dbt.assets.Beverages import dimension_data, beverage_fact_data, dbt_beverage_data


@job(tags={"source": "Beverages"})
def beverage_dim_job():
    Dimension_data_success = dimension_data()

    Fact_data_success = beverage_fact_data(Dimension_data_success)

    dbt_beverage_data(Fact_data_success)
