import boto3
import json
from datetime import datetime


BUCKET_NAME = "news-ai-etl-raw"
REGION      = "us-east-1"

# S3 client - reads credentials from environment variables automatically
# AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
s3 = boto3.client("s3", region_name=REGION)


def save_to_s3(source_name, articles):
    """
    Save a list of articles to S3 as a JSON file.

    S3 path structure:
      raw/{source}/year=YYYY/month=MM/day=DD/fetch_TIMESTAMP.json

    Example:
      raw/yahoo_finance/year=2026/month=08/day=01/fetch_20260801_143000.json
    """

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
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=content,
        ContentType="application/json",
    )

    print(f"  Saved to s3://{BUCKET_NAME}/{s3_key}")
    print(f"  Articles saved: {len(articles)}")


def save_all_to_s3(results):
    """
    results is a dict like: { "yahoo_finance": [...], "cnbc_markets": [...] }
    Loop through each source and save to S3.
    """

    for source_name, articles in results.items():
        print(f"\nSaving {source_name} to S3 ...")
        save_to_s3(source_name, articles)
