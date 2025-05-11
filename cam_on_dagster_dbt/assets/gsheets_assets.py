from dagster import asset
import os
import pandas as pd
import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
import dlt

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
    client = gspread.authorize(creds)
    sheet = client.open(os.getenv("GOOGLE_SHEET_NAME")).sheet1
    data = sheet.get_all_records()
    df = pd.DataFrame(data)

    if df.empty:
        context.log.warning("No data found.")
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
