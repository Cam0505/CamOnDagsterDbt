from dagster import asset, OpExecutionContext
import os
import requests
from dotenv import load_dotenv
from pathlib import Path
import dlt
import time
import subprocess
from typing import Iterator, Dict, Any
import duckdb

load_dotenv(dotenv_path="/workspaces/CamOnDagster/.env")

BASE_URL = "https://rickandmortyapi.com/api"

# Configuration for resources: endpoint -> primary key
RESOURCE_CONFIG: dict[str, str] = {
    "character": "id",
    "episode": "id",
    "location": "id"
}


def paginate_all(endpoint: str) -> list[Dict[str, Any]]:
    """Fetch all pages for a given Rick and Morty endpoint."""
    results = []
    url = f"{BASE_URL}/{endpoint}"
    while url:
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()
        results.extend(data.get("results", []))
        url = data.get("info", {}).get("next")
    return results


def get_existing_count(table_name: str) -> int:
    db_path = os.getenv("DESTINATION__DUCKDB__CREDENTIALS")
    if not db_path:
        raise ValueError(
            "Missing DESTINATION__DUCKDB__CREDENTIALS in environment.")
    con = duckdb.connect(database=db_path)
    try:
        result = con.execute(
            f"SELECT COUNT(*) FROM rick_and_morty_data.{table_name}").fetchone()
        return result[0] if result else 0
    except Exception:
        return 0  # Table might not exist yet
    finally:
        con.close()


def make_resource(endpoint: str, primary_key: str):
    table_name = endpoint

    @dlt.resource(name=table_name, write_disposition="merge", primary_key=primary_key)
    def _resource(context: OpExecutionContext) -> Iterator[Dict]:
        state = dlt.current.source_state().setdefault(table_name, {
            "count": 0,
            "last_run_status": None
        })

        existing_count = get_existing_count(table_name)
        data = paginate_all(endpoint)
        new_count = len(data)

        if existing_count < state["count"]:
            context.log.info(
                f"⚠️ Table `{table_name}` row count dropped from {state['count']} to {existing_count}. Forcing reload.")
        elif new_count == state["count"]:
            context.log.info(f"🔁 SKIPPED LOAD: `{table_name}` — No new data.")
            state["last_run_status"] = "skipped_no_new_data"
            return

        context.log.info(
            f"✅ New data for `{table_name}`: {state['count']} ➝ {new_count}")
        state["count"] = new_count
        state["last_run_status"] = "loaded"
        yield from data

    return _resource


@dlt.source
def rick_and_morty_source(context: OpExecutionContext):
    for endpoint, primary_key in RESOURCE_CONFIG.items():
        yield make_resource(endpoint, primary_key)(context)


@asset(compute_kind="python", group_name="RickAndMorty", tags={"source": "RickAndMorty"})
def rick_and_morty_asset(context: OpExecutionContext) -> bool:
    """Loads characters, episodes, and locations from Rick and Morty API using DLT."""
    context.log.info("🚀 Starting DLT pipeline for Rick and Morty API")
    pipeline = dlt.pipeline(
        pipeline_name="rick_and_morty_pipeline",
        destination="duckdb",
        dataset_name="rick_and_morty_data"
    )

    source = rick_and_morty_source(context)
    try:
        load_info = pipeline.run(source)
        context.log.info(f"✅ Load Successful: {load_info}")
        return bool(load_info)
    except Exception as e:
        context.log.error(f"❌ Pipeline run failed: {e}")
        return False


@asset(deps=["rick_and_morty_asset"], group_name="RickAndMorty", tags={"source": "RickAndMorty"})
def dbt_rick_and_morty_data(context: OpExecutionContext, rick_and_morty_asset: bool) -> None:
    """Runs dbt models for Rick and Morty API after loading data."""
    if not rick_and_morty_asset:
        context.log.warning(
            "\n⚠️  WARNING: DBT SKIPPED\n"
            "📉 No data was loaded from Rick and Morty API.\n"
            "🚫 Skipping dbt run.\n"
        )
        return

    DBT_PROJECT_DIR = Path("/workspaces/CamOnDagster/dbt").resolve()
    context.log.info(f"📁 DBT Project Directory: {DBT_PROJECT_DIR}")

    start = time.time()
    try:
        result = subprocess.run(
            "dbt build --select source:rick_and_morty+",
            shell=True,
            cwd=DBT_PROJECT_DIR,
            capture_output=True,
            text=True,
            check=True
        )
        duration = round(time.time() - start, 2)
        context.log.info(f"✅ dbt build completed in {duration}s")
        context.log.info(result.stdout)
    except subprocess.CalledProcessError as e:
        context.log.error(f"❌ dbt build failed:\n{e.stdout}\n{e.stderr}")
        raise
