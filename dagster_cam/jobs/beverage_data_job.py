from dagster import job, define_asset_job
from dagster_cam.assets.Beverages import ingredients_table, alcoholic_table, beverages_table, glass_table, beverage_fact_data, dbt_beverage_data


# @job(tags={"source": "Beverages"})
# def beverage_dim_job():
#     Dimension_data_success = dimension_data()

#     Fact_data_success = beverage_fact_data(Dimension_data_success)

#     dbt_beverage_data(Fact_data_success)


beverage_dim_job = define_asset_job(
    name="beverage_dim_job",
    # Dagster auto-infers dependency on `ingredients_table`, `alcoholic_table`, `beverages_table`, and `glass_table`
    selection=[ingredients_table, alcoholic_table,
               beverages_table, glass_table,
               beverage_fact_data, dbt_beverage_data]
)
