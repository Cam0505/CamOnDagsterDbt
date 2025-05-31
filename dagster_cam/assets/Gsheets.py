from dagster import asset, OpExecutionContext
import os
import pandas as pd
import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
import dlt
from datetime import datetime, time
from zoneinfo import ZoneInfo
from path_config import ENV_FILE, CREDENTIALS, DLT_PIPELINE_DIR


load_dotenv(dotenv_path=ENV_FILE)

# Prevent Accidental Manual Execution Outside of ASX Hours


def is_within_asx_hours() -> bool:
    now_sydney = datetime.now(ZoneInfo("Australia/Sydney"))
    if now_sydney.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        return False

    market_open = time(10, 0)
    market_close = time(16, 0)

    return market_open <= now_sydney.time() <= market_close


@dlt.source
def gsheet_finance_source(logger=None):
    @dlt.resource(write_disposition="merge", primary_key="id", name="gsheets_finance")
    def gsheet_finance_resource():
        # Initialize state within the source context
        state = dlt.current.source_state().setdefault("gsheet_finance", {
            "latest_ts": None,
            "last_run": None,
            "processed_records": 0,
            "last_run_status": None
        })
        if logger:
            logger.info(f"Current state: {state}")

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
                state["last_run_status"] = "skipped_empty_data"
                if logger:
                    logger.warning("No data found in sheet")
                return

            if "DateTime" not in data[0]:
                state["last_run_status"] = "skipped_missing_datetime"
                if logger:
                    logger.warning("DateTime column missing")
                return

            # Process data and track timestamps
            df = pd.DataFrame(data)
            df['DateTime'] = pd.to_datetime(
                df['DateTime']).dt.tz_localize('UTC')
            latest_gsheet_ts = df['DateTime'].max()
            if logger:
                logger.info(f"Latest data timestamp: {latest_gsheet_ts}")

            # Check for new data
            if state["latest_ts"]:
                latest_state_ts = pd.to_datetime(state["latest_ts"])
                buffered_ts = latest_state_ts + pd.Timedelta(minutes=30)

                # if latest_gsheet_ts <= buffered_ts:
                #     state.update({
                #         "last_run": datetime.now(ZoneInfo("UTC")).isoformat(),
                #         "last_run_status": "skipped_no_new_data"
                #     })
                #     if logger:
                #         logger.info(
                #             f"\n🔁 SKIPPED LOAD:\n"
                #             f"📅 GSheet timestamp: {latest_gsheet_ts}\n"
                #             f"📦 Buffered DLT state timestamp: {buffered_ts}\n"
                #             f"⏳ Reason: No new data within 30-minute window.\n"
                #             f"{'-'*45}"
                #         )
                #     return

            # Update state
            state.update({
                "latest_ts": latest_gsheet_ts.isoformat(),
                "last_run": datetime.now(ZoneInfo("UTC")).isoformat(),
                "processed_records": len(df),
                "last_run_status": "success"
            })

            if logger:
                logger.info(f"Loading {len(df)} new records")
            yield df.to_dict('records')

        except Exception as e:
            state["last_run_status"] = f"failed: {str(e)}"
            if logger:
                logger.error(f"Processing failed: {e}")
            raise

    return gsheet_finance_resource


@asset(compute_kind="python", group_name="gSheets", tags={"source": "gSheets"})
def gsheet_finance_data(context: OpExecutionContext) -> bool:
    # if not is_within_asx_hours():
    #     context.log.info("\n\nOutside ASX trading hours - skipping")
    #     return False

    pipeline = dlt.pipeline(
        pipeline_name="gsheets_to_duckdb",
        destination=os.getenv("DLT_DESTINATION", "MotherDuck"),
        dataset_name="google_sheets_data",
        pipelines_dir=str(DLT_PIPELINE_DIR),
        dev_mode=False
    )

    # Get the source
    source = gsheet_finance_source(context.log)
    try:
        load_info = pipeline.run(source)

        status = source.state.get(
            'gsheet_finance', {}).get('last_run_status', '')

        if status == 'skipped_no_new_data':
            context.log.info(f"\n⏭️ resource skipped — no data loaded.")
            return False
        elif status == 'success':
            context.log.info(f"\n✅ Resource loaded: {load_info}")
            return True
        else:
            context.log.error(
                f"\n💥 All resources failed to load: {status}")
            return False
    except Exception as e:
        context.log.error(f"\n❌ Pipeline run failed: {e}")
        return False


@asset(
    deps=["gsheet_finance_data"],
    group_name="gSheets",
    tags={"source": "gSheets"},
    required_resource_keys={"dbt"}
)
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

    try:
        invocation = context.resources.dbt.cli(
            ["build", "--select", "source:gsheets+"],
            context=context
        )

        # Wait for dbt to finish and get the full stdout log
        invocation.wait()
        return
    except Exception as e:
        context.log.error(f"dbt build failed:\n{e}")
        raise
