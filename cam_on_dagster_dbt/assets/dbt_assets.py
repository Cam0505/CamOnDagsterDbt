from pathlib import Path
from dagster_dbt import load_assets_from_dbt_project, DbtProject

# Define paths
DBT_PROJECT_PATH = Path(__file__).joinpath("..", "..", "dbt").resolve()
DBT_PACKAGED_PATH = Path(__file__).joinpath("..", "dbt-project").resolve()

# Load dbt project
camon_dbt_project = DbtProject(
    project_dir=DBT_PROJECT_PATH,
    packaged_project_dir=DBT_PACKAGED_PATH,
)

# Load Dagster assets from dbt
camon_dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_PATH,
    profiles_dir=DBT_PROJECT_PATH / "profiles"
)
