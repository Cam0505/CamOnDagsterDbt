

import subprocess
from dagster import asset
from pathlib import Path
# from dagster._core.execution.context.compute import OpExecutionContext
from dagster import OpExecutionContext
from dagster_dbt import dbt_assets, DbtCliResource


@asset(deps=["meals_fact_data"])
def dbt_meals_data(context: OpExecutionContext):
    """Runs the dbt command after loading the data from Beverage API."""
    DBT_PROJECT_DIR = Path("/workspaces/CamOnDagster/dbt").resolve()
    context.log.info(f"DBT Project Directory: {DBT_PROJECT_DIR}")

    result = subprocess.run(
        "dbt build --select source:meals+",
        shell=True,
        cwd=DBT_PROJECT_DIR,
        capture_output=True,
        text=True
    )

    context.log.info(result.stdout)
    if result.stderr:
        context.log.error(result.stderr)

    if result.returncode != 0:
        raise Exception(f"dbt build failed with code {result.returncode}")
