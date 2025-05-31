from dagster import define_asset_job
from dagster_cam.assets.airlines import airlines_asset, airports_asset, routes_asset, planes_asset


airline_job = define_asset_job(
    name="airline_job",
    # Dagster auto-infers dependency on `airlines`
    selection=[airlines_asset, airports_asset, routes_asset, planes_asset]
)
