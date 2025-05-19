from dagster import job
from cam_on_dagster_dbt.assets.youtube import youtube_pipeline


@job(tags={"source": "Youtube"})
def Youtube_Job():
    # First, run the youtube_pipeline asset
    youtube_pipeline()
