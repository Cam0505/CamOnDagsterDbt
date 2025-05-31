
import os
from dagster import AssetExecutionContext
from path_config import DBT_DIR
from dagster_dbt import dbt_assets, DbtCliResource, DbtProject
dbt_project = DbtProject(
    project_dir=DBT_DIR,
    profiles_dir=DBT_DIR,
)


# Define paths
dbt = DbtCliResource(project_dir=os.fspath(DBT_DIR))


dbt_manifest_path = DBT_DIR.joinpath("target", "manifest.json")


@dbt_assets(manifest=dbt_manifest_path,
            io_manager_key="io_manager", name="camon_dbt_assets")
def camon_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """Run dbt build command and return result."""
    yield from dbt.cli(["build"], context=context).stream()
