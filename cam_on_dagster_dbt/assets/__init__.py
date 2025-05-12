# assets/__init__.py

# This file can be left empty or used to explicitly import assets if needed.
# It allows for the assets package to be recognized and for easier imports in other modules.

from .dbt_assets import camon_dbt_assets
from .gsheets_assets import gsheet_finance_data
from .gsheets_customdbt import run_dbt_command
from .Beverages import beverage_dim_data, dimension_data, beverage_fact_data, dbt_beverage_data
from .Meals import meals_dim_data, meals_dimension_data, meals_fact_data, dbt_meals_data
