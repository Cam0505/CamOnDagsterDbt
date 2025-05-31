import os
import re
from datetime import date


def sanitize_filename(value: str) -> str:
    # Replace all non-word characters (anything other than letters, digits, underscore) with underscore
    no_whitespace = ''.join(value.split())
    ascii_only = no_whitespace.encode("ascii", errors="ignore").decode()
    return re.sub(r"[^\w\-_\.]", "_", ascii_only)


def json_converter(o):
    if isinstance(o, date):
        return o.isoformat()
    return str(o)
