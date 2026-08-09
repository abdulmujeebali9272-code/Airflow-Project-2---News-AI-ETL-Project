import boto3
import json
from datetime import datetime


BUCKET_NAME = "new-ai-eti-project"
REGION      = "eu-north-1"


def get_client():
    """
    Local/CLI S3 client - reads credentials from environment variables
    (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) via boto3's default chain.
    Used when running run.py directly, outside Airflow.
    """
    return boto3.client("s3", region_name=REGION)


def save_to_s3(source_name, articles, s3_client=None):
    """
    Save a list of articles to S3 as a JSON file.

    S3 path structure:
      raw/{source}/year=YYYY/month=MM/day=DD/fetch_TIMESTAMP.json

    Example:
      raw/yahoo_finance/year=2026/month=08/day=01/fetch_20260801_143000.json

    s3_client is optional - pass one in (e.g. from Airflow's S3Hook) to reuse
    an existing connection. Defaults to a plain boto3 client for local runs.
    """

    s3_client = s3_client or get_client()
    now = datetime.now()

    # Build the S3 key (the file path inside the bucket)
    s3_key = (
        f"raw/{source_name}/"
        f"year={now.strftime('%Y')}/"
        f"month={now.strftime('%m')}/"
        f"day={now.strftime('%d')}/"
        f"fetch_{now.strftime('%Y%m%d_%H%M%S')}.json"
    )

    # Convert articles list to a JSON string
    content = json.dumps(articles, indent=2)

    # Upload to S3
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=content,
        ContentType="application/json",
    )

    print(f"  Saved to s3://{BUCKET_NAME}/{s3_key}")
    print(f"  Articles saved: {len(articles)}")


def save_all_to_s3(results, s3_client=None):
    """
    results is a dict like: { "yahoo_finance": [...], "cnbc_markets": [...] }
    Loop through each source and save to S3.
    """

    s3_client = s3_client or get_client()

    for source_name, articles in results.items():
        print(f"\nSaving {source_name} to S3 ...")
        save_to_s3(source_name, articles, s3_client=s3_client)
