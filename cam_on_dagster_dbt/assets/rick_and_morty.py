from dagster import asset, OpExecutionContext
import os
# import requests
from dotenv import load_dotenv
from pathlib import Path
import dlt
import time
import subprocess
from typing import Iterator, Dict, Any
import duckdb
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator
from dlt.sources.helpers.rest_client.client import RESTClient

load_dotenv(dotenv_path="/workspaces/CamOnDagster/.env")

BASE_URL = "https://rickandmortyapi.com/api"

# Configuration for resources: endpoint -> primary key
RESOURCE_CONFIG: dict[str, str] = {
    "character": "id",
    "episode": "id",
    "location": "id"
}


def get_existing_count(table_name: str, context) -> int:
    db_path = os.getenv("MOTHERDUCK")
    if not db_path:
        raise ValueError(
            "Missing MOTHERDUCK in environment.")
    if table_name not in RESOURCE_CONFIG.keys():
        raise ValueError("Unauthorized access attempt.")

    con = duckdb.connect(database=db_path)
    try:
        result = con.execute(
            f"SELECT COUNT(*) FROM rick_and_morty_data.{table_name}").fetchone()
        return result[0] if result else 0
    except Exception as e:
        context.log.warn(f"Failed to get row count for `{table_name}`: {e}")
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

        existing_count = get_existing_count(table_name, context)
        client = RESTClient(
            base_url=f"{BASE_URL}/",
            paginator=JSONLinkPaginator(
                next_url_path="info.next"
            )
        )
        # Only fetch first page to check count
        try:
            response = client.session.get(f"{BASE_URL}/{endpoint}")
            response.raise_for_status()
            first_page = response.json()
            info = first_page.get("info", {})
            new_count = info.get("count", 0)
        except Exception as e:
            context.log.error(
                f"❌ Failed to fetch API count for `{table_name}`: {e}")
            state["last_run_status"] = "failed"
            raise

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
        state["last_run_status"] = "success"
        context.log.info(
            f"📊 Loading `{table_name}` data from Rick and Morty API...")
        for page in client.paginate(endpoint):
            yield page

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
        destination=os.getenv("DLT_DESTINATION", "duckdb"),
        dataset_name="rick_and_morty_data"
    )

    source = rick_and_morty_source(context)
    try:
        load_info = pipeline.run(source)

        statuses = [source.state.get(resource, {}).get(
            "last_run_status") for resource in RESOURCE_CONFIG.keys()]

        if all(s == "skipped_no_new_data" for s in statuses):
            context.log.info(
                "⏭️ All resources skipped — no data loaded.")
            return False
        elif all(s == "failed" for s in statuses):
            context.log.error(
                "💥 All resources failed to load — check API or network.")
            return False

        loaded_count = sum(1 for s in statuses if s == "success")
        context.log.info(f"✅ Number of resources loaded: {loaded_count}")

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
