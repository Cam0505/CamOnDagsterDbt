from pathlib import Path
from dagster import asset, Output, OutputContext, AssetExecutionContext
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets
import os

# Define paths
DBT_PROJECT_DIR = Path("/workspaces/CamOnDagster/dbt").resolve()
dbt = DbtCliResource(project_dir=os.fspath(DBT_PROJECT_DIR))


dbt_manifest_path = DBT_PROJECT_DIR.joinpath("target", "manifest.json")


@dbt_assets(manifest=dbt_manifest_path,
            io_manager_key="io_manager", name="camon_dbt_assets")
def camon_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """Run dbt build command and return result."""
    yield from dbt.cli(["build"], context=context).stream()
