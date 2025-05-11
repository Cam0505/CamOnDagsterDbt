# cam_on_dagster_dbt/assets/gsheets_customdbt.py

import subprocess
from dagster import asset
from pathlib import Path
# from dagster._core.execution.context.compute import OpExecutionContext
from dagster import OpExecutionContext
from dagster_dbt import dbt_assets, DbtCliResource


@asset(deps=["gsheet_finance_data"])
def run_dbt_command(context: OpExecutionContext):
    """Runs the dbt command after loading the data from Google Sheets."""
    DBT_PROJECT_DIR = Path("/workspaces/CamOnDagster/dbt").resolve()
    context.log.info(f"DBT Project Directory: {DBT_PROJECT_DIR}")

    result = subprocess.run(
        "dbt run --select base_gsheets_finance+",
        shell=True,
        cwd=DBT_PROJECT_DIR,
        capture_output=True,
        text=True
    )

    context.log.info("STDOUT:\n" + result.stdout)
    context.log.error("STDERR:\n" + result.stderr)

    if result.returncode != 0:
        raise Exception(
            f"dbt command failed with return code {result.returncode}")

    return result.stdout
