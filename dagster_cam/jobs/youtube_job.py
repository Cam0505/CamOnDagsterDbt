from dagster import job
from dagster_cam.assets.youtube import youtube_pipeline


@job(tags={"source": "Youtube"})
def Youtube_Job():
    # First, run the youtube_pipeline asset
    youtube_pipeline()
