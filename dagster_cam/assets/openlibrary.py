import dlt
import os
from dagster import asset, AssetExecutionContext
from pandas import DataFrame
import time
import json
from typing import Iterator, Dict
from dotenv import load_dotenv
from path_config import ENV_FILE, REQUEST_CACHE_DIR, DLT_PIPELINE_DIR
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.sources.helpers.rest_client.client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
from dlt.sources.helpers import requests
from dlt.pipeline.exceptions import PipelineNeverRan
from helper_functions import sanitize_filename

load_dotenv(dotenv_path=ENV_FILE)


BASE_URL = "https://openlibrary.org/search.json"

# Define your search terms and topics
SEARCH_TOPICS = [
    "Python Programming",
    "Apache Airflow",
    "Prefect Pythonic",
    "SQL Programming",
    "Dbtlabs",
    "Terraform",
    "DuckDB",
    "PostgreSQL"
]

PAGE_LIMIT = 100  # Number of results per page


@dlt.source
def openlibrary_dim_source(context, current_table):

    @dlt.resource(name="books", write_disposition="merge", primary_key="key")
    def resource_func():
        state = dlt.current.source_state().setdefault("books", {
            "count": {},
            "last_run_status": {}
        })

        for term in SEARCH_TOPICS:

            if current_table is None:
                current_table_count = 0
            else:
                current_table_count = current_table.get(term, 0)
            context.log.info(
                f"\n📊 Current table count for '{term}': {current_table_count}")
            try:

                initial_response = requests.get(
                    BASE_URL, params={"q": term, "limit": PAGE_LIMIT}, timeout=15)
                initial_response.raise_for_status()
                count = initial_response.json().get("numFound", 0)

                # Step 2: Calculate max pages
                max_pages = (count + PAGE_LIMIT - 1) // PAGE_LIMIT

                previous_count = state["count"].get(term, 0)

                if previous_count < count:
                    context.log.info(
                        f"\n⚠️ Detected more data from API ({count})"
                        f" than previous count ({previous_count}). Forcing reload."
                    )
                    # Need to fix this, to tired, merge is preventing duplicate rows
                elif current_table_count > 0 and count == previous_count:
                    context.log.info(
                        f"🔁 SKIPPED LOAD for {term}:\n"
                        f"📊 Current table count: {current_table_count}\n"
                        f"📅 Previous filtered count: {previous_count}\n"
                        f"📦 Current filtered count: {count}\n"
                        f"⏳ No new data. Skipping..."
                    )
                    state["last_run_status"][term] = "skipped_no_new_data"
                    continue

                client = RESTClient(
                    base_url="https://openlibrary.org/search.json?",
                    paginator=PageNumberPaginator(
                        base_page=1,
                        page=1,
                        total_path=None,
                        page_param="page",
                        stop_after_empty_page=True,
                        maximum_page=max_pages,
                    ), data_selector="docs"
                )

                context.log.info(
                    f"\n📚 Loading paginated data for '{term}' ({current_table_count} ➝ {count})")

                for page in client.paginate(params={"q": term, "limit": 100}, data_selector="docs"):
                    for doc in page.response.json()["docs"]:
                        # Main Table
                        yield {
                            "search_term": term,
                            "key": doc.get("key"),
                            "title": doc.get("title"),
                            "ebook_access": doc.get("ebook_access"),
                            "first_publish_year": doc.get("first_publish_year"),
                            "has_fulltext": doc.get("has_fulltext", False),
                            "authors": [{"name": a} for a in doc.get("author_name", [])],
                            "languages": [{"code": l} for l in doc.get("language", [])]
                        }

                state["count"][term] = count
                state["last_run_status"][term] = "success"

            except Exception as e:
                context.log.error(f"❌ Fetch failed for '{term}': {e}")
                state["last_run_status"][term] = "failed"
                return

    return resource_func


@asset(compute_kind="python", group_name="OpenLibrary", tags={"source": "OpenLibrary"})
def openlibrary_books_asset(context: AssetExecutionContext) -> DataFrame | None:

    pipeline = dlt.pipeline(
        pipeline_name="openlibrary_books",
        destination=os.environ.get(
            "DLT_DESTINATION") or os.getenv("DLT_DESTINATION"),
        pipelines_dir=str(DLT_PIPELINE_DIR),
        dataset_name="openlibrary_data"
    )

    try:
        dataset = pipeline.dataset()["books"].df()
        if dataset is not None:
            row_count = dataset.groupby("search_term").size().to_dict()
            context.log.info(f"Grouped Row Counts:\n{row_count}")
    except PipelineNeverRan:
        context.log.warning(
            "⚠️ No previous runs found for this pipeline. Assuming first run.")
        row_count = None
    except DatabaseUndefinedRelation:
        context.log.warning(
            "⚠️ Table Doesn't Exist. Assuming truncation.")
        row_count = None

    source = openlibrary_dim_source(context, row_count)

    try:
        load_info = pipeline.run(source)

        statuses = {
            term: source.state.get("books", {}).get(
                "last_run_status", {}).get(term, '')
            for term in SEARCH_TOPICS
        }

        if all(status == "skipped_no_new_data" for status in statuses.values()):
            context.log.info("⏭️ All resources skipped — no new data.")
            return None
        elif all(status == "failed" for status in statuses.values()):
            context.log.error("💥 All resources failed to load.")
            return None

        successful_terms = [term for term,
                            status in statuses.items() if status == "success"]
        if not successful_terms:
            return None

        context.log.info(
            f"✅ Successfully loaded resources for: {successful_terms}")

        # Fetch dataset again and filter for successful terms
        full_df = pipeline.dataset()["books"].df()
        if full_df is not None:
            filtered_df = full_df[full_df["search_term"].isin(
                successful_terms)]
            return DataFrame({
                "search_term": filtered_df["search_term"],
                "key": filtered_df["key"]
            })
        else:
            context.log.warning("⚠️ No data returned after successful load.")
            return None

    except Exception as e:
        context.log.error(f"❌ Pipeline run failed: {e}")
        return None


@dlt.resource(
    name="book_subjects",
    write_disposition="merge",
    primary_key=["work_id", "subject"],
    table_name="subjects"
)
def openlibrary_work_metadata(context, books_df) -> Iterator[Dict]:
    os.makedirs(REQUEST_CACHE_DIR, exist_ok=True)

    for row in books_df.itertuples():
        work_key = getattr(row, "key", None)
        if not work_key or not work_key.startswith("/works/"):
            continue

        work_id = work_key.split("/")[-1]
        cache_path = REQUEST_CACHE_DIR / f"{sanitize_filename(work_id)}.json"

        if cache_path.exists():
            with open(cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        else:
            try:
                url = f"https://openlibrary.org/works/{work_id}.json"
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                data = response.json()

                with open(cache_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)

                time.sleep(0.5)  # rate-limiting protection
            except Exception as e:
                context.log.error(
                    f"Failed to fetch or cache data for work_id {work_id}: {e}")
                yield {"work_id": work_id, "subjects": [], "error": str(e)}
                continue

        subjects = data.get("subjects", [])
        if subjects:
            for subject in subjects:
                if subject:  # filters None, empty strings, etc.
                    yield {"work_id": work_id, "subject": subject}


@asset(compute_kind="python", deps=["openlibrary_books_asset"],
       group_name="OpenLibrary", tags={"source": "OpenLibrary"})
def openlibrary_subjects_asset(context: AssetExecutionContext, books_df) -> bool:

    if books_df is None or books_df.empty:
        context.log.warning(
            "📭 Skipping metadata pipeline: books_df is None or empty.")
        return False

    context.log.info(
        f"🚀 Running DLT pipeline to fetch and load work metadata...")

    pipeline = dlt.pipeline(
        pipeline_name="openlibrary_subjects",
        destination=os.environ.get(
            "DLT_DESTINATION") or os.getenv("DLT_DESTINATION"),
        pipelines_dir=str(DLT_PIPELINE_DIR),
        dataset_name="openlibrary_data"
    )

    load_info = pipeline.run(openlibrary_work_metadata(context, books_df))
    context.log.info(f"✅ DLT load complete: {load_info}")
    return True


@asset(deps=["openlibrary_subjects_asset"], group_name="OpenLibrary",
       tags={"source": "OpenLibrary"}, required_resource_keys={"dbt"})
def dbt_openlibrary_data(context: AssetExecutionContext, openlibrary_subjects_asset: bool) -> None:
    """Runs the dbt command after loading the data from Geo API."""

    if not openlibrary_subjects_asset:
        context.log.warning(
            "\n⚠️  WARNING: DBT SKIPPED\n"
            "📉 No data was loaded from OpenLibrary API.\n"
            "🚫 Skipping dbt run.\n"
            "----------------------------------------"
        )
        # Print all active threads
        return

    try:
        invocation = context.resources.dbt.cli(
            ["build", "--select", "source:openlibrary+"],
            context=context
        )

        # Wait for dbt to finish and get the full stdout log
        invocation.wait()
        return
    except Exception as e:
        context.log.error(f"dbt build failed:\n{e}")
        raise
