from dagster import asset, AssetExecutionContext
import os
# import requests
from dotenv import load_dotenv
from pathlib import Path
import dlt
import time
import subprocess
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


def make_resource(table_name: str, primary_key: str, existing_count: int):

    @dlt.resource(name=table_name, write_disposition="merge", primary_key=primary_key)
    def _resource(context: AssetExecutionContext):
        state = dlt.current.source_state().setdefault(table_name, {
            "count": 0,
            "last_run_status": None
        })

        client = RESTClient(
            base_url=f"{BASE_URL}/",
            paginator=JSONLinkPaginator(
                next_url_path="info.next"
            )
        )

        # Only fetch first page to check count
        try:
            response = client.session.get(
                f"{BASE_URL}/{table_name}", timeout=15)
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
        for page in client.paginate(table_name):
            yield page

    return _resource


@dlt.source
def rick_and_morty_source(context: AssetExecutionContext, current_counts):
    for endpoint, primary_key in RESOURCE_CONFIG.items():
        yield make_resource(endpoint, primary_key, current_counts.get(endpoint, 0))(context)


@asset(compute_kind="python", group_name="RickAndMorty", tags={"source": "RickAndMorty"})
def rick_and_morty_asset(context: AssetExecutionContext) -> bool:
    """Loads characters, episodes, and locations from Rick and Morty API using DLT."""
    context.log.info("🚀 Starting DLT pipeline for Rick and Morty API")

    pipeline = dlt.pipeline(
        pipeline_name="rick_and_morty_pipeline",
        destination=os.getenv("DLT_DESTINATION", "duckdb"),
        dataset_name="rick_and_morty_data"
    )

    row_counts = pipeline.dataset().row_counts().df()
    if row_counts is not None:
        row_counts_dict = dict(
            zip(row_counts["table_name"], row_counts["row_count"]))
    else:
        context.log.warning(
            "⚠️ No tables found yet in dataset — assuming first run.")
        row_counts_dict = {}

    source = rick_and_morty_source(context, row_counts_dict)
    try:
        pipeline.run(source)

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

        return True
    except Exception as e:
        context.log.error(f"❌ Pipeline run failed: {e}")
        return False


@asset(deps=["rick_and_morty_asset"], group_name="RickAndMorty",
       tags={"source": "RickAndMorty"}, required_resource_keys={"dbt"})
def dbt_rick_and_morty_data(context: AssetExecutionContext, rick_and_morty_asset: bool) -> None:
    """Runs dbt models for Rick and Morty API after loading data."""

    if not rick_and_morty_asset:
        context.log.warning(
            "\n⚠️  WARNING: DBT SKIPPED\n"
            "📉 No data was loaded from Rick and Morty API.\n"
            "🚫 Skipping dbt run.\n"
        )
        return

    try:
        invocation = context.resources.dbt.cli(
            ["build", "--select", "source:rick_and_morty+"],
            context=context
        )

        # Wait for dbt to finish and get the full stdout log
        invocation.wait()
        return
    except Exception as e:
        context.log.error(f"dbt build failed:\n{e}")
        raise
