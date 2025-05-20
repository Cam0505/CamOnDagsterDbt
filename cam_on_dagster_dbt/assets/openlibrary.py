import dlt
from dlt.sources.helpers.requests import get
import os
from dagster import asset, OpExecutionContext
import threading
from typing import Iterator, Dict
import subprocess
import time
from dotenv import load_dotenv
load_dotenv(dotenv_path="/workspaces/CamOnDagster/.env")


BASE_URL = "https://openlibrary.org/search.json"

# Define your search terms and topics
SEARCH_TOPICS: dict[str, list[str]] = {
    "Python": ["Python", "python_books"],
    "Apache Airflow": ["Apache Airflow", "apache_airflow_books"],
    "Data Engineering": ["Data Engineering", "data_engineering_books"],
    "Data Warehousing": ["Data Warehousing", "data_warehousing_books"],
    "SQL": ["SQL", "sql_books"]

}


def get_existing_count(table_name: str, context) -> int:
    try:
        pipeline = dlt.current.pipeline()
        with pipeline.sql_client() as client:
            result = client.execute_sql(
                f"SELECT COUNT(*) FROM openlibrary_data.{table_name}")
            count = result[0][0] if result else 0
            context.log.info(
                f"🔍 Existing row count for `{table_name}`: {count}")
            return count
    except Exception as e:
        context.log.warning(
            f"⚠️ Could not get count for {table_name}: {str(e)}")
        return 0  # Assume table doesn't exist yet


def create_resource(term: str, topic: str, resource_name: str, context: OpExecutionContext):

    @dlt.resource(name=resource_name, write_disposition="merge", primary_key="key")
    def resource_func() -> Iterator[Dict]:
        state = dlt.current.source_state().setdefault(resource_name, {
            "count": 0,
            "last_run_status": None
        })

        current_table_count = get_existing_count(resource_name, context)
        try:
            response = get(BASE_URL, params={"q": term, "limit": 100})
            data = response.json()
        except Exception as e:
            context.log.error(f"❌ Fetch failed for '{term}': {e}")
            state["last_run_status"] = "failed"
            return

        # Prepare filtered rows first
        filtered_rows = []
        for book in data["docs"]:
            subject_list = book.get("subject", [])
            subject_str = " ".join(
                subject_list).lower() if subject_list else ""
            title = book.get("title", "").lower()

            if topic.lower() in title or topic.lower() in subject_str:
                filtered_rows.append({
                    "search_term": term,
                    "topic_filter": topic,
                    "title": book.get("title"),
                    "author_name": ", ".join(book.get("author_name", [])),
                    "publish_year": book.get("first_publish_year"),
                    "isbn": ", ".join(book.get("isbn", [])) if book.get("isbn") else None,
                    "edition_count": book.get("edition_count"),
                    "key": book.get("key"),
                    "subject_raw": subject_list,
                    "subject_str": subject_str
                })

        filtered_count = len(filtered_rows)
        previous_count = state["count"]

        if current_table_count < previous_count:
            context.log.info(
                f"⚠️ Detected fewer rows in DuckDB table '{resource_name}' ({current_table_count}) "
                f"than previous filtered count ({previous_count}). Forcing reload."
            )
        elif filtered_count == previous_count:
            context.log.info(
                f"🔁 SKIPPED LOAD for {term}:\n"
                f"📅 Previous filtered count: {previous_count}\n"
                f"📦 Current filtered count: {filtered_count}\n"
                f"⏳ No new data. Skipping..."
            )
            state["last_run_status"] = "skipped_no_new_data"
            return

        context.log.info(
            f"✅ New filtered data for '{term}': {previous_count} ➝ {filtered_count}"
        )
        state["count"] = filtered_count
        state["last_run_status"] = "success"

        try:
            yield from filtered_rows
        except Exception as e:
            context.log.error(f"❌ Failed to yield data for {term}: {e}")
            state["last_run_status"] = "failed"

    return resource_func


@dlt.source
def openlibrary_dim_source(context: OpExecutionContext):
    for term, (topic, resource_name) in SEARCH_TOPICS.items():
        yield create_resource(term, topic, resource_name, context)


@asset(compute_kind="python", group_name="OpenLibrary", tags={"source": "OpenLibrary"})
def openlibrary_books_asset(context: OpExecutionContext) -> bool:

    context.log.info("Starting DLT pipeline...")
    pipeline = dlt.pipeline(
        pipeline_name="openlibrary_incremental",
        destination=os.getenv("DLT_DESTINATION", "duckdb"),
        dataset_name="openlibrary_data"
    )

    source = openlibrary_dim_source(context)

    try:
        load_info = pipeline.run(source)

        statuses = [source.state.get(resource, {}).get(
            "last_run_status") for (junk, resource) in SEARCH_TOPICS.values()]

        if all(s == "skipped_no_new_data" for s in statuses):
            context.log.info(
                "⏭️ All resources skipped or failed — no data loaded.")
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


@asset(deps=["openlibrary_books_asset"], group_name="OpenLibrary", tags={"source": "OpenLibrary"})
def dbt_openlibrary_data(context: OpExecutionContext, openlibrary_books_asset: bool) -> None:
    """Runs the dbt command after loading the data from Geo API."""

    if not openlibrary_books_asset:
        context.log.warning(
            "\n⚠️  WARNING: DBT SKIPPED\n"
            "📉 No data was loaded from OpenLibrary API.\n"
            "🚫 Skipping dbt run.\n"
            "----------------------------------------"
        )
        # Print all active threads
        context.log.info(
            f"🧵 Active threads: {[t.name for t in threading.enumerate()]}")
        return

    DBT_PROJECT_DIR = os.path.abspath("/workspaces/CamOnDagster/dbt")
    context.log.info(f"DBT Project Directory: {DBT_PROJECT_DIR}")

    start = time.time()
    try:
        result = subprocess.run(
            ["dbt", "build", "--select", "source:openlibrary+"],
            cwd=DBT_PROJECT_DIR,
            capture_output=True,
            text=True,
            check=True
        )
        duration = round(time.time() - start, 2)
        context.log.info(f"dbt build completed in {duration}s")
        context.log.info(result.stdout)
    except subprocess.CalledProcessError as e:
        context.log.error(f"dbt build failed:\n{e.stdout}\n{e.stderr}")
        raise
