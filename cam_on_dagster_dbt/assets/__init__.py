# assets/__init__.py

# This file can be left empty or used to explicitly import assets if needed.
# It allows for the assets package to be recognized and for easier imports in other modules.

from .dbt_assets import camon_dbt_assets
from .gsheets_assets import gsheet_finance_data
from .gsheets_customdbt import run_dbt_command
from .beverage_dim_assets import beverage_dim_data
from .beverage_dim_data_assets import dimension_data
from .beverage_fact_assets import beverage_fact_data
from .beverage_dbtBuild import dbt_beverage_data
from .meals_dim_assets import meals_dim_data
from .meals_dim_data_assets import meals_dimension_data
from .meals_fact_assets import meals_fact_data
from .meals_dbtBuild import dbt_meals_data
