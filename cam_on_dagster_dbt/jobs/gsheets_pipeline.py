import os
import subprocess
import pandas as pd
import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from dagster import job, op, Definitions, ScheduleDefinition

import dlt

# Load environment variables from .env (adjust path if needed)
load_dotenv(dotenv_path="../../.env")


@op
def extract_sheet_name() -> str:
    return os.getenv("GOOGLE_SHEET_NAME")


@op
def extract_data_from_gsheet(context, sheet_name: str) -> pd.DataFrame:
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']

    creds = Credentials.from_service_account_file(
        os.getenv("CREDENTIALS_FILE"), scopes=scope)
    client = gspread.authorize(creds)

    sheet = client.open(sheet_name).sheet1
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    context.log.info(f"Extracted {len(df)} rows from Google Sheets")
    return df


@op
def load_data_to_duckdb(context, df: pd.DataFrame) -> bool:
    if df.empty:
        context.log.warning("No data found in Google Sheets.")
        return False

    pipeline = dlt.pipeline(
        pipeline_name="gsheets_to_duckdb",
        destination="duckdb",
        dataset_name="google_sheets_data",
        dev_mode=False
    )

    load_info = pipeline.run(
        df,
        table_name="gsheet_Finance",
        write_disposition="merge",
        primary_key="Id"
    )

    context.log.info(f"Load info: {load_info}")
    return True


@op
def run_dbt_command(context, should_run: bool):
    if not should_run:
        context.log.info("Skipping dbt run.")
        return "Skipped"

    DBT_PROJECT_DIR = "/workspaces/CamOnDagster/dbt"
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


@job
def gsheets_financial_job():
    sheet_name = extract_sheet_name()
    df = extract_data_from_gsheet(sheet_name)
    did_load = load_data_to_duckdb(df)
    run_dbt_command(did_load)


every_3_minutes_schedule = ScheduleDefinition(
    job=gsheets_financial_job,
    cron_schedule="*/3 * * * *",
    execution_timezone="UTC",
    name="every_3_minutes_schedule",
)
