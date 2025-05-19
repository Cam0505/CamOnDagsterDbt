import os
import requests
import json
from dotenv import load_dotenv

# Load API key
load_dotenv("/workspaces/CamOnDagster/.env")
API_KEY = os.getenv("GOOGLE_API_KEY")
BASE_URL = "https://www.googleapis.com/youtube/v3"
CHANNEL_ID = "UCQvq-0fss4lNrmIz7gcPLtQ"  # HowNot2

# Step 1: Get the uploads playlist ID for the channel


def get_uploads_playlist_id(channel_id):
    url = f"{BASE_URL}/channels"
    params = {
        "part": "contentDetails",
        "id": channel_id,
        "key": API_KEY
    }
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    uploads_id = resp.json()[
        "items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    return uploads_id

# Step 2: Get the first 5 videos from that playlist


def get_first_5_video_ids(playlist_id):
    url = f"{BASE_URL}/playlistItems"
    params = {
        "part": "contentDetails",
        "playlistId": playlist_id,
        "maxResults": 5,
        "key": API_KEY
    }
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    items = resp.json()["items"]
    return [item["contentDetails"]["videoId"] for item in items]

# Step 3: Fetch full video details


def get_video_details(video_ids):
    url = f"{BASE_URL}/videos"
    params = {
        "part": "snippet,statistics,contentDetails",
        "id": ",".join(video_ids),
        "key": API_KEY
    }
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    return resp.json()


if __name__ == "__main__":
    uploads_id = get_uploads_playlist_id(CHANNEL_ID)
    video_ids = get_first_5_video_ids(uploads_id)
    video_data = get_video_details(video_ids)

    print(json.dumps(video_data, indent=2))
