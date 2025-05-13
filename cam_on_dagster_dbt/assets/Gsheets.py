from dagster import asset, OpExecutionContext
import subprocess
from pathlib import Path
import os
import pandas as pd
import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
import dlt
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

load_dotenv(dotenv_path="/workspaces/CamOnDagster/.env")


@asset(compute_kind="python")
def gsheet_finance_data(context) -> bool:
    creds = Credentials.from_service_account_file(
        os.getenv("CREDENTIALS_FILE"),
        scopes=[
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
    )
    # Authorize the client
    client = gspread.authorize(creds)
    SHEET_NAME = os.getenv('GOOGLE_SHEET_NAME')
    if not SHEET_NAME:
        raise ValueError(
            "Environment variable GOOGLE_SHEET_NAME is not set or empty.")

    # Open the Google Sheet
    sheet = client.open(SHEET_NAME).sheet1
    # Get all records from the sheet
    data = sheet.get_all_records()
    # Convert to DataFrame
    df = pd.DataFrame(data)

    if df.empty:
        context.log.warning("No data found.")
        return False

    if "DateTime" not in df.columns:
        context.log.warning("'DateTime' column not found in sheet.")
        return False

    try:
        df["DateTime"] = pd.to_datetime(df["DateTime"]).dt.tz_localize(
            ZoneInfo("Australia/Melbourne"))
        latest_ts = df["DateTime"].max()
        now = datetime.now(ZoneInfo("Australia/Melbourne"))
        if latest_ts > now - timedelta(minutes=30):
            context.log.info(
                f"\n🔁 SKIPPED LOAD:\n"
                f"📅 Latest timestamp: {latest_ts}\n"
                f"🕒 Current time: {now}\n"
                f"⏳ Reason: Timestamp is less than 30 minutes old.\n"
                f"{'-'*40}")
            return False
    except Exception as e:
        context.log.error(f"Failed to parse 'DateTime' column: {e}")
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
    context.log.info(f"Loaded data: {load_info}")
    return True


@asset(deps=["gsheet_finance_data"])
def gsheet_dbt_command(context: OpExecutionContext, gsheet_finance_data: bool) -> None:
    """Runs the dbt command after loading the data from Google Sheets."""

    if not gsheet_finance_data:
        context.log.warning(
            "\n⚠️  WARNING: DBT SKIPPED\n"
            "📉 No data was loaded from Google Sheets.\n"
            "🚫 Skipping dbt run.\n"
            "----------------------------------------"
        )
        return

    DBT_PROJECT_DIR = Path("/workspaces/CamOnDagster/dbt").resolve()
    context.log.info(f"DBT Project Directory: {DBT_PROJECT_DIR}")

    start = time.time()
    try:
        result = subprocess.run(
            "dbt run --select base_gsheets_finance+",
            shell=True,
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
