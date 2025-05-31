from dagster import asset, OpExecutionContext
import os
import requests
from dotenv import load_dotenv
import dlt
import time
from path_config import ENV_FILE, DLT_PIPELINE_DIR


load_dotenv(dotenv_path=ENV_FILE)
TABLE_PARAMS = {
    "category": ["c=list", "strCategory"],
    "country": ["a=list", "strArea"],
    "ingredients": ["i=list", "strIngredient"]
}


def create_resource(table_name, param, value_key, context):
    @dlt.resource(name=table_name, write_disposition="replace")
    def resource_func():
        url = f"https://www.themealdb.com/api/json/v1/1/list.php?{param}"

        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            if "meals" not in data or data["meals"] is None:
                context.log.warning(
                    f"No 'meals' found in API response for {table_name}")
                return
            meals = data["meals"]
            state = dlt.current.source_state().setdefault(table_name, {
                "processed_records": 0,
                "last_run_status": None,
                "values": []
            })

            current_list = [d.get(value_key) for d in meals if value_key in d]
            previous_list = state.get("values", [])
            current_value = len(current_list)
            previous_value = state.get("processed_records")

            if (current_value == previous_value):
                context.log.info(
                    f"🔁 SKIPPED LOAD for {table_name}:\n"
                    f"📅 Previous: {previous_value}\n"
                    f"📦 Current: {current_value}\n"
                    f"⏳ No new data for {table_name}. Skipping..."
                )
                state["last_run_status"] = "skipped_no_new_data"
                return

            state["processed_records"] = current_value
            state["values"] = current_list
            state["last_run_status"] = "success"

            for meal in meals:
                yield meal
        except requests.exceptions.RequestException as e:
            context.log.error(
                f"Request failed for {table_name}: {e}", exc_info=True)
            raise
        except ValueError as e:
            context.log.error(
                f"JSON decode failed for {table_name}: {e}", exc_info=True)
            raise
    return resource_func


@dlt.source
def meal_dim_source(context: OpExecutionContext):

    for table_name, (param, value_key) in TABLE_PARAMS.items():
        yield create_resource(table_name, param, value_key, context)


@asset(compute_kind="python", group_name="Meals", tags={"source": "Meals"})
def meals_dim_data(context) -> dict:
    """Loads beverage dimension tables if row counts differ from expected."""
    context.log.info("Starting DLT pipeline...")
    pipeline = dlt.pipeline(
        pipeline_name="meals_pipeline",
        destination=os.getenv("DLT_DESTINATION", "motherduck"),
        pipelines_dir=str(DLT_PIPELINE_DIR),
        dataset_name="meals_data",
        dev_mode=False,
    )
    source = meal_dim_source(context)
    context.log.info("Running pipeline...")
    load_info = pipeline.run(source)
    context.log.info(f"Pipeline finished. Load info: {load_info}")
    all_values = {}

    for table in TABLE_PARAMS.keys():
        if table in source.state:
            all_values[table] = source.state[table]["values"]
        else:
            context.log.warn(f"Table '{table}' not found in source.state")

    return all_values


def create_dimension_resource(table_name, config, values, context):
    @dlt.resource(name=config["resource_name"], write_disposition="replace")
    def resource_func():
        categories = values.get(table_name, [])
        state = dlt.current.source_state().setdefault(config["resource_name"], {
            "processed_records": 0,
            "last_run_status": None
        })

        total_records = 0
        for value in categories:
            if not value:
                continue
            url = f"https://www.themealdb.com/api/json/v1/1/filter.php?{config['query_param']}={value}"

            try:
                response = requests.get(url, timeout=15)
                response.raise_for_status()  # Raise exception on error
                meals = response.json().get("meals")
                if meals is None:
                    # context.log.warning(
                    #     f"No meals found for {config['query_param']}='{value}'")
                    continue
                for meal in meals:
                    if isinstance(meal, dict):  # Ensure it's a dictionary
                        meal[config["source_key"]] = value
                        yield meal
                        total_records += 1
            except Exception as e:
                context.log.warning(
                    f"❌ Failed to fetch drinks for value '{value}': {e}")
                state["last_run_status"] = "failed"
                return
        previous_value = state.get("processed_records", 0)

        if total_records == previous_value or state["last_run_status"] == "failed":
            context.log.info(
                f"🔁 SKIPPED LOAD for {config['resource_name']}:\n"
                f"📅 Previous: {previous_value}\n"
                f"📦 Current: {total_records}\n"
                f"⏳ No new data for {config['resource_name']}. Skipping..."
            )
            state["last_run_status"] = "skipped_no_new_data"
            return
        else:
            state["processed_records"] = total_records
            state["last_run_status"] = "success"

    return resource_func


@dlt.source
def dimension_data_source(context: OpExecutionContext, values: dict):

    DIMENSION_CONFIG = {
        "ingredients": {
            "sql_column": "strIngredient",
            "query_param": "i",
            "source_key": "source_ingredient",
            "resource_name": "ingredient_table"
        },
        "country": {
            "sql_column": "strArea",
            "query_param": "a",
            "source_key": "source_country",
            "resource_name": "country_table"
        },
        "category": {
            "sql_column": "strCategory",
            "query_param": "c",
            "source_key": "source_category",
            "resource_name": "category_table"
        }
    }

    for table_name, config in DIMENSION_CONFIG.items():
        context.log.info(f"Creating resource: {table_name}")
        yield create_dimension_resource(table_name, config, values, context)


@asset(compute_kind="python", deps=["meals_dim_data"], group_name="Meals", tags={"source": "Meals"})
def meals_dimension_data(context, meals_dim_data: dict) -> bool:
    if not meals_dim_data:
        context.log.warning(
            "\n⚠️  WARNING: meals_dim_data SKIPPED\n"
            "📉 No data was loaded from meals_dim_data.\n"
            "🚫 Skipping meals_dimension_data run.\n"
            "----------------------------------------"
        )
        return False
    context.log.info("Starting DLT pipeline...")
    pipeline = dlt.pipeline(
        pipeline_name="meals_pipeline",
        destination=os.getenv("DLT_DESTINATION", "duckdb"),
        pipelines_dir=str(DLT_PIPELINE_DIR),
        dataset_name="meals_data",
        dev_mode=False
    )

    source = dimension_data_source(context, meals_dim_data)
    try:
        load_info = pipeline.run(source)
        context.log.info(f"✅ Load Successful: {load_info}")
        return bool(load_info)
    except Exception as e:
        context.log.error(f"❌ Pipeline run failed: {e}")
        return False


@asset(compute_kind="python", deps=["meals_dimension_data"], group_name="Meals", tags={"source": "Meals"})
def meals_fact_data(context, meals_dimension_data: bool) -> bool:

    @dlt.resource(name="consumption", write_disposition="append")
    def meals_api():
        if not meals_dimension_data:
            context.log.warning(
                "\n⚠️  WARNING: dimension_data SKIPPED\n"
                "📉 No data was loaded from dimension_data.\n"
                "🚫 Skipping beverage_fact_data run.\n"
                "----------------------------------------"
            )
            return False
        url = f"https://www.themealdb.com/api/json/v1/1/random.php"

        for i in range(20):
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                meals = response.json().get("meals")
                if not meals:
                    context.log.warning(
                        f"No meals returned in iteration {i+1}")
                    continue
                yield meals

                time.sleep(0.2)  # Prevent throttling
            except requests.RequestException as e:
                context.log.error(
                    f"Request failed on iteration {i+1}: {e}", exc_info=True)
            except Exception as e:
                context.log.error(
                    f"Unexpected error on iteration {i+1}: {e}", exc_info=True)

    @dlt.source
    def meals():
        return meals_api()

    try:
        pipeline = dlt.pipeline(
            pipeline_name="meals_pipeline",
            destination=os.getenv("DLT_DESTINATION", "duckdb"),
            dataset_name="meals_data",
            pipelines_dir=str(DLT_PIPELINE_DIR),
            dev_mode=False
        )

        load_info = pipeline.run(meals())
        context.log.info(f"DLT pipeline run complete: {load_info}")
        return bool(load_info)

    except Exception as e:
        context.log.error("DLT pipeline run failed", exc_info=True)
        raise
    finally:
        # Ensure that the connection is closed even if an error occurs
        return True


@asset(deps=["meals_fact_data"], group_name="Meals",
       tags={"source": "Meals"}, required_resource_keys={"dbt"})
def dbt_meals_data(context: OpExecutionContext, meals_fact_data: bool) -> None:
    """Runs the dbt command after loading the data from Beverage API."""
    if not meals_fact_data:
        context.log.warning(
            "\n⚠️  WARNING: meals_fact_data SKIPPED\n"
            "📉 No data was loaded from meals_fact_data.\n"
            "🚫 Skipping DBT Build.\n"
            "----------------------------------------"
        )
        return

    try:
        invocation = context.resources.dbt.cli(
            ["build", "--select", "source:meals+"],
            context=context
        )

        # Wait for dbt to finish and get the full stdout log
        invocation.wait()
        return
    except Exception as e:
        context.log.error(f"dbt build failed:\n{e}")
        raise
