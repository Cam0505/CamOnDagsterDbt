from dagster import asset, OpExecutionContext
import subprocess
from pathlib import Path
import os
import pandas as pd
import gspread
import duckdb
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
        # Ensure 'DateTime' is correctly parsed to pandas datetime
        latest_gsheet_ts = pd.to_datetime(df['DateTime'])

        if isinstance(latest_gsheet_ts, pd.Series):
            latest_gsheet_ts = latest_gsheet_ts.max()

        context.log.info(f"Latest GSheet timestamp: {latest_gsheet_ts}")

        # Make latest_gsheet_ts timezone-aware if it is not
        if latest_gsheet_ts.tzinfo is None:
            latest_gsheet_ts = latest_gsheet_ts.tz_localize('UTC')

        # ✅ Query DuckDB for the latest existing timestamp
        db_path = os.getenv("DESTINATION__DUCKDB__CREDENTIALS")
        if not db_path:
            raise ValueError(
                "Missing DuckDB path in DESTINATION__DUCKDB__CREDENTIALS")

        con = duckdb.connect(db_path)
        result = con.execute(""" 
            SELECT MAX(date_time) AS max_dt
            FROM google_sheets_data_staging.gsheet_finance
            WHERE date_time IS NOT NULL
        """).fetchone()
        con.close()

        latest_db_ts_raw = result[0] if result and result[0] else None
        if latest_db_ts_raw:
            latest_db_ts = pd.to_datetime(latest_db_ts_raw)

            # Make latest_db_ts timezone-aware (using UTC timezone as an example)
            if latest_db_ts.tzinfo is None:
                latest_db_ts = latest_db_ts.tz_localize('UTC')

            context.log.info(f"Latest DB timestamp: {latest_db_ts}")

        buffered_db_ts = latest_db_ts + pd.Timedelta(minutes=30)
        # Now compare both timestamps
        if latest_gsheet_ts <= buffered_db_ts:
            context.log.info(
                f"\n🔁 SKIPPED LOAD:\n"
                f"📅 Latest GSheet timestamp: {latest_gsheet_ts}\n"
                f"📦 Latest DB timestamp (+30min buffer): {buffered_db_ts}\n"
                f"⏳ Reason: GSheet data is not newer than what's already in the DB.\n"
                f"{'-'*45}"
            )
            return False

    except Exception as e:
        context.log.error(f"Error during datetime comparison: {e}")
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
