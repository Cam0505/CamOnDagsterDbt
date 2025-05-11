from dagster import AssetSelection, define_asset_job
from .assets.dbt_assets import camon_dbt_assets
from .gsheets_pipeline import gsheets_financial_job


selected_assets = [*camon_dbt_assets]

cams_job = define_asset_job(
    name="cams_job",
    selection=selected_assets,

)

# all_assets_job = define_asset_job(
#     name="intial_populate",
#     selection=AssetSelection.all()
# )
