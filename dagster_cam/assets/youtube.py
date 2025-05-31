from dagster import asset, OpExecutionContext
import os
import requests
from pathlib import Path
from dotenv import load_dotenv
from typing import Iterator, Dict, Any, List
import dlt
import time
import subprocess
from datetime import datetime


load_dotenv(dotenv_path="/workspaces/CamOnDagster/.env")


API_KEY = os.getenv('GOOGLE_API_KEY')
if not API_KEY:
    raise ValueError("Missing GOOGLE_API_KEY in environment variables.")
BASE_URL = "https://www.googleapis.com/youtube/v3"


def _retry_request(url: str, params: Dict[str, Any], context: OpExecutionContext, max_retries=3, backoff=2) -> Dict[str, Any]:
    attempt = 0
    while attempt <= max_retries:
        try:
            context.log.debug(f"Request attempt {attempt + 1}: {url}")
            resp = requests.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            context.log.warning(f"Attempt {attempt + 1} failed: {e}")
            attempt += 1
            if attempt > max_retries:
                context.log.error(
                    f"All {max_retries} retries failed for {url}")
                raise
            sleep_time = backoff ** attempt
            context.log.info(f"Retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)
    return {}

# -------------------------
# Resource: Channel Metadata
# -------------------------


@dlt.resource(name="channels", write_disposition="merge", primary_key="id")
def fetch_channels(channel_ids: List[str], context: OpExecutionContext) -> Iterator[Dict[str, Any]]:
    context.log.info(
        f"Starting channels fetch for {len(channel_ids)} channels")
    state = dlt.current.source_state().setdefault("channels", {
        "LastDateSeen": None,
        "count": 0,
        "last_run_status": None
    })

    ids_str = ",".join(channel_ids)
    params = {
        "key": API_KEY,
        "part": "snippet,statistics,contentDetails",
        "id": ids_str
    }

    data = _retry_request(f"{BASE_URL}/channels", params, context)
    state["count"] = data.get("pageInfo", {}).get("totalResults", 0)
    items = data.get("items", [])
    context.log.info(f"Retrieved {len(items)} channels")

    for item in items:  # Per Channel
        channel_id = item["id"]
        context.log.debug(f"Processing channel {channel_id}")
        yield {
            "id": channel_id,
            "title": item["snippet"]["title"],
            "description": item["snippet"]["description"],
            "published_at": item["snippet"]["publishedAt"],
            "subscriber_count": item["statistics"].get("subscriberCount"),
            "video_count": item["statistics"].get("videoCount"),
            "view_count": item["statistics"].get("viewCount"),
            "uploads_playlist_id": item["contentDetails"]["relatedPlaylists"]["uploads"]
        }


# -------------------------
# Resource: Channel Videos
# -------------------------
@dlt.resource(name="videos", write_disposition="merge", primary_key="id")
def fetch_channel_videos(channel_ids: List[str], context: OpExecutionContext) -> Iterator[Dict[str, Any]]:
    context.log.info("Starting videos fetch")
    state = dlt.current.source_state().setdefault("videos", {
        "LastDateSeen": None,
        "count": 0,
        "last_run_status": None
    })
    last_published = state.get("LastDateSeen", None)
    context.log.info(f"Last published video date from state: {last_published}")

    channels = list(fetch_channels(channel_ids, context))
    context.log.info(f"Processing videos for {len(channels)} channels")

    video_count = 0
    for channel in channels:
        uploads_playlist_id = channel["uploads_playlist_id"]
        channel_id = channel["id"]
        context.log.info(
            f"Fetching videos for channel {channel_id} from playlist {uploads_playlist_id}")
        next_page_token = None
        page_count = 0

        while True:
            page_count += 1
            params = {
                "key": API_KEY,
                "part": "snippet,contentDetails,statistics,status",
                "playlistId": uploads_playlist_id,
                "maxResults": 50,
                "pageToken": next_page_token
            }

            try:
                context.log.debug(
                    f"Fetching page {page_count} for playlist {uploads_playlist_id}")
                data = _retry_request(
                    f"{BASE_URL}/playlistItems", params, context)
            except Exception as e:
                context.log.error(
                    f"Failed to fetch videos for playlist {uploads_playlist_id}: {str(e)}")
                break

            items = data.get("items", [])
            context.log.info(
                f"Retrieved {len(items)} videos from page {page_count}")

            for item in items:
                video_id = item["contentDetails"]["videoId"]
                published_at = item["contentDetails"]["videoPublishedAt"]

                if last_published and published_at <= last_published:
                    context.log.debug(
                        f"Skipping video {video_id} (published at {published_at})")
                    continue

                video_count += 1
                context.log.debug(
                    f"Processing video {video_id} published at {published_at}")
                yield {
                    "id": video_id,
                    "published_at": published_at,
                    "title": item["snippet"]["title"],
                    "description": item["snippet"]["description"],
                    "duration": item["contentDetails"].get("duration"),
                    "like_count": item["statistics"].get("likeCount"),
                    "comment_count": item["statistics"].get("commentCount"),
                    "view_count": item["statistics"].get("viewCount"),
                    "channel_id": channel_id,
                }

                if last_published is None or published_at > last_published:
                    state["LastDateSeen"] = published_at
                    context.log.debug(
                        f"Updated last published date to {published_at}")

            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                context.log.info(
                    f"No more pages for playlist {uploads_playlist_id}")
                break
    state["count"] = video_count
    context.log.info(f"Finished processing {video_count} new videos")


# -------------------------
# Resource: Channel Playlists
# -------------------------
@dlt.resource(name="playlists", write_disposition="merge", primary_key="id")
def fetch_playlists(channel_ids: List[str], context: OpExecutionContext) -> Iterator[Dict[str, Any]]:
    context.log.info("Starting playlists fetch")
    state = dlt.current.source_state().setdefault("playlists", {
        "LastDateSeen": None,
        "count": 0,
        "last_run_status": None
    })

    playlist_count = 0
    for channel_id in channel_ids:
        context.log.info(f"Fetching playlists for channel {channel_id}")
        next_page_token = None
        page_count = 0

        while True:
            page_count += 1
            params = {
                "key": API_KEY,
                "part": "snippet,contentDetails",
                "channelId": channel_id,
                "maxResults": 50,
                "pageToken": next_page_token
            }
            try:
                data = _retry_request(f"{BASE_URL}/playlists", params, context)
            except Exception:
                break

            items = data.get("items", [])
            context.log.info(
                f"Retrieved {len(items)} playlists from page {page_count}")

            for item in items:
                playlist_count += 1
                playlist_id = item["id"]
                context.log.debug(f"Processing playlist {playlist_id}")
                yield {
                    "id": playlist_id,
                    "title": item["snippet"]["title"],
                    "description": item["snippet"].get("description"),
                    "published_at": item["snippet"]["publishedAt"],
                    "item_count": item["contentDetails"]["itemCount"],
                    "channel_id": channel_id
                }

            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                context.log.info(f"No more pages for channel {channel_id}")
                break
    state["count"] = playlist_count
    context.log.info(f"Finished processing {playlist_count} playlists")


# -------------------------
# Resource: Channel Activities
# -------------------------
@dlt.resource(name="activities", write_disposition="merge", primary_key="id")
def fetch_activities(channel_ids: List[str], context: OpExecutionContext) -> Iterator[Dict[str, Any]]:
    context.log.info("Starting activities fetch")
    state = dlt.current.source_state().setdefault("activities", {
        "LastDateSeen": None,
        "count": 0,
        "last_run_status": None
    })
    last_seen = state["LastDateSeen"]
    context.log.info(f"Last seen activity date from state: {last_seen}")

    activity_count = 0
    for channel_id in channel_ids:
        context.log.info(f"Fetching activities for channel {channel_id}")
        next_page_token = None
        page_count = 0

        while True:
            page_count += 1
            params = {
                "key": API_KEY,
                "part": "snippet,contentDetails",
                "channelId": channel_id,
                "maxResults": 50,
                "pageToken": next_page_token
            }

            try:
                data = _retry_request(
                    f"{BASE_URL}/activities", params, context)
            except Exception:
                break

            items = data.get("items", [])
            context.log.info(
                f"Retrieved {len(items)} activities from page {page_count}")

            for item in items:
                activity_id = item["id"]
                published_at = item["snippet"]["publishedAt"]
                activity_type = item["snippet"]["type"]
                video_id = item["contentDetails"].get("upload", {}).get(
                    "videoId") if activity_type == "upload" else None
                playlist_id = item.get("contentDetails", {}).get("addToPlaylist", {}).get(
                    "playlistId") if activity_type == "addToPlaylist" else None

                if last_seen and published_at <= last_seen:
                    context.log.debug(
                        f"Skipping activity {activity_id} (published at {published_at})")
                    continue

                activity_count += 1
                context.log.debug(
                    f"Processing activity {activity_id} published at {published_at}")
                yield {
                    "id": activity_id,
                    "title": item["snippet"]["title"],
                    "type": activity_type,
                    "video_id": video_id,
                    "description": item["snippet"]["description"],
                    "playlist_id": playlist_id,
                    "published_at": published_at,
                    "channel_id": channel_id
                }

                if last_seen is None or published_at > last_seen:
                    state["LastDateSeen"] = published_at
                    context.log.debug(
                        f"Updated last seen activity date to {published_at}")

            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                context.log.info(f"No more pages for channel {channel_id}")
                break
    state["count"] = activity_count
    context.log.info(f"Finished processing {activity_count} new activities")


@dlt.source
def youtube_source(context: OpExecutionContext):
    channel_ids = [
        "UCnbbOxqwPK2lfkEP3FzIf2Q",  # Hard Is Easy
        "UCQvq-0fss4lNrmIz7gcPLtQ",  # HowNOT2
    ]

    context.log.info(
        f"Initializing YouTube source for {len(channel_ids)} channels")
    yield fetch_channels(channel_ids, context)
    yield fetch_channel_videos(channel_ids, context)
    yield fetch_playlists(channel_ids, context)
    yield fetch_activities(channel_ids, context)


@asset(compute_kind="python", group_name="youtube_pipeline", tags={"source": "youtube_pipeline"})
def youtube_pipeline(context: OpExecutionContext):
    context.log.info("Starting YouTube pipeline execution")

    pipeline = dlt.pipeline(
        pipeline_name="youtube_pipeline",
        destination=os.getenv("DLT_DESTINATION", "duckdb"),
        dataset_name="youtube_data"
    )

    source = youtube_source(context)
    try:
        context.log.info("Running pipeline...")
        load_info = pipeline.run(source)
        context.log.info(f"✅ Load successful: {load_info}")
        context.log.info(
            f"Load package info: {pipeline.get_load_package_info(load_info.loads_ids[0])}")
        return bool(load_info)
    except Exception as e:
        context.log.error(f"❌ Pipeline run failed: {e}")
        context.log.error(f"Error details: {str(e)}", exc_info=True)
        return False
