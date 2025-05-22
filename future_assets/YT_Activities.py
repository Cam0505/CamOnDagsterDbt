import os
import requests
import json
from dotenv import load_dotenv

load_dotenv(dotenv_path="/workspaces/CamOnDagster/.env")

API_KEY = os.getenv("GOOGLE_API_KEY")
if not API_KEY:
    raise ValueError("Missing GOOGLE_API_KEY in environment variables.")

BASE_URL = "https://www.googleapis.com/youtube/v3"
CHANNEL_ID = "UCQvq-0fss4lNrmIz7gcPLtQ"  # HowNOT2


def fetch_activities(channel_id, max_results=3):
    params = {
        "key": API_KEY,
        "part": "snippet,contentDetails",
        "channelId": channel_id,
        "maxResults": max_results
    }
    response = requests.get(f"{BASE_URL}/activities", params=params)
    response.raise_for_status()
    return response.json()


def main():
    data = fetch_activities(CHANNEL_ID)
    print(json.dumps(data, indent=4))


if __name__ == "__main__":
    main()
