from dagster import asset, AssetExecutionContext, Output, MetadataValue
import os
import pandas as pd
import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from datetime import datetime, time
from zoneinfo import ZoneInfo
from path_config import ENV_FILE, CREDENTIALS, DBT_DIR
from dagster_dbt import dbt_assets, DbtCliResource, DbtProject
dbt_project = DbtProject(
    project_dir=DBT_DIR,
    profiles_dir=DBT_DIR,
)


load_dotenv(dotenv_path=ENV_FILE)

# Prevent Accidental Manual Execution Outside of ASX Hours


def is_within_asx_hours() -> bool:
    now_sydney = datetime.now(ZoneInfo("Australia/Sydney"))
    if now_sydney.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        return False

    market_open = time(10, 0)
    market_close = time(16, 0)

    return market_open <= now_sydney.time() <= market_close


@asset(compute_kind="python", group_name="gSheets", tags={"source": "gSheets"})
def gsheet_finance_data(context: AssetExecutionContext) -> Output:

    try:
        # Load data from Google Sheets
        creds = Credentials.from_service_account_file(
            CREDENTIALS,
            scopes=[
                'https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive'
            ]
        )
        client = gspread.authorize(creds)
        SHEETNAME = os.getenv("GOOGLE_SHEET_NAME")
        if not SHEETNAME:
            raise ValueError(
                "Missing GOOGLE_SHEET_NAME in .env file")
        sheet = client.open(SHEETNAME).sheet1
        data = sheet.get_all_records()

        if not data:
            return Output(
                pd.DataFrame(),
                metadata={
                    "source": MetadataValue.text("Google Sheets"),
                    "sheet_name": MetadataValue.text(SHEETNAME),
                    "row_count": MetadataValue.int(0),
                    "columns": MetadataValue.text("No data found"),
                }
            )

        # Process data and track timestamps
        df = pd.DataFrame(data)
        df['DateTime'] = pd.to_datetime(
            df['DateTime'], errors='coerce', utc=True)
        assert pd.api.types.is_datetime64_any_dtype(
            df['DateTime']), "DateTime column is not datetime64"

        return Output(df,
                      metadata={
                          "source": MetadataValue.text("Google Sheets"),
                          "sheet_name": MetadataValue.text(SHEETNAME),
                          "row_count": MetadataValue.int(len(df)),
                          "columns": MetadataValue.text(", ".join(df.columns)),
                      })

    except Exception as e:
        # state["last_run_status"] = f"failed: {str(e)}"
        if context:
            context.log.error(f"Processing failed: {e}")
        raise


@dbt_assets(
    name="dbt_models",
    manifest=dbt_project.manifest_path,
    select="source:gsheets+"
)
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
