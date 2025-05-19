import os
import requests
import json
from dotenv import load_dotenv

load_dotenv(dotenv_path="/workspaces/CamOnDagster/.env")

API_KEY = os.getenv('GOOGLE_API_KEY')
if not API_KEY:
    raise ValueError("Missing GOOGLE_API_KEY in environment variables.")

BASE_URL = "https://www.googleapis.com/youtube/v3"
CHANNEL_IDS = [
    "UCnbbOxqwPK2lfkEP3FzIf2Q",  # Hard Is Easy
    "UCQvq-0fss4lNrmIz7gcPLtQ",  # HowNOT2
]


def fetch_channels(channel_ids):
    ids_str = ",".join(channel_ids)
    params = {
        "key": API_KEY,
        "part": "snippet,statistics,contentDetails",
        "id": ids_str
    }
    response = requests.get(f"{BASE_URL}/channels", params=params)
    response.raise_for_status()
    return response.json()


def main():
    data = fetch_channels(CHANNEL_IDS)
    print(json.dumps(data, indent=4))


if __name__ == "__main__":
    main()
