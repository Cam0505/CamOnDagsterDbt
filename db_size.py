

import os
from dotenv import load_dotenv


load_dotenv(dotenv_path="/workspaces/CamOnDagster/.env")

db_path = os.getenv("DESTINATION__DUCKDB__CREDENTIALS")
if not db_path:
    raise ValueError(
        "Missing DESTINATION__DUCKDB__CREDENTIALS in environment.")

size_bytes = os.path.getsize(db_path)
size_mb = size_bytes / (1024 * 1024)

print(f"DuckDB file size: {size_bytes} bytes ({size_mb:.2f} MB)")
