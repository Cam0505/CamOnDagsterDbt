# camonprefect/path_config.py
from pathlib import Path
import os
import sys
import logging
from functools import lru_cache


@lru_cache(maxsize=1)
def get_project_root():
    """Resolves the correct root path across all environments."""
    search_paths = [
        Path("/workspaces/CamOnDagster"),  # Devcontainer
        Path(__file__).parent.parent,      # Local dev
        Path.cwd()                         # Fallback
    ]

    for path in search_paths:
        try:
            if (path / "dbt").exists() and (path / "dagster_cam").exists():
                return path.resolve()
        except (PermissionError, OSError) as e:
            logging.warning(f"Couldn't access {path} - {str(e)}")

    cwd = Path.cwd()
    raise FileNotFoundError(
        "Project root not found! Checked:\n"
        f"- Possible roots: {[str(p) for p in search_paths]}\n"
        f"- Current directory: {str(cwd)}\n"
        f"- Contents: {[f.name for f in cwd.iterdir() if f.is_dir()]}\n"
        "Required structure: must contain 'dbt/' and 'dagster_cam/' subdirectories"
    )


# Standardized paths
PROJECT_ROOT = get_project_root()
DBT_DIR = PROJECT_ROOT / "dbt"
CREDENTIALS = PROJECT_ROOT / "keys" / "credentials.json"
ENV_FILE = PROJECT_ROOT / ".env"
REQUEST_CACHE_DIR = PROJECT_ROOT / "request_cache"
DAGSTER_DIR = PROJECT_ROOT / "dagster_cam"
DLT_PIPELINE_DIR = DAGSTER_DIR / ".dlt"
ASSETS_DIR = DAGSTER_DIR / "assets"
JOBS_DIR = DAGSTER_DIR / "jobs"

# Set env vars for DLT to pick up secrets/configs from the right location
os.environ["DLT_DATA_DIR"] = str(DLT_PIPELINE_DIR)
os.environ["DLT_CONFIG_DIR"] = str(DLT_PIPELINE_DIR)
os.environ["DLT_SECRETS_DIR"] = str(DLT_PIPELINE_DIR)

# Optional: Make PROJECT_ROOT accessible to subprocesses
os.environ["PROJECT_ROOT"] = str(PROJECT_ROOT)
