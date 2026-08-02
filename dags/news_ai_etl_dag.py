"""
Airflow DAG version of run.py.

Same 6 steps, just split into tasks so Airflow can schedule, retry, and
show progress for each one individually instead of running them as one
long script.

Credentials come from Airflow instead of .env:
  - S3        -> Airflow Connection "aws_default"        (Admin > Connections)
  - Snowflake -> Airflow Connection "snowflake_default"   (Admin > Connections)
  - Gemini    -> Airflow Variable   "gemini_api_key"      (Admin > Variables)

See README.md "Airflow Setup" section for exact steps to create these.

Needs these provider packages installed wherever Airflow runs:
  pip install apache-airflow-providers-snowflake apache-airflow-providers-amazon
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from ingestion.rss_fetcher import fetch_all
from ingestion.scraper import add_text_to_articles
from storage.s3_storage import save_all_to_s3
from storage.snowflake_loader import save_all_to_snowflake
from storage.silver_loader import bronze_to_silver
from storage.llm_enricher import enrich_staged_news


default_args = {
    "owner": "ayan",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="news_ai_etl",
    description="Fetch financial news, scrape full text, land in S3 + Snowflake bronze/silver, enrich with Gemini",
    schedule="0 */6 * * *",   #
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["news", "etl", "snowflake", "llm"],
)
def news_ai_etl():

    @task
    def fetch_rss():
        """Step 1: poll the RSS feeds. Small payload - just title/link/description."""
        return fetch_all()

    @task
    def scrape_articles(results: dict):
        """Step 2: visit each article link and add full body text."""
        for source, articles in results.items():
            results[source] = add_text_to_articles(articles)
        return results

    @task
    def save_to_s3(results: dict):
        """Step 3: land raw JSON in S3, partitioned by source/year/month/day."""
        s3_client = S3Hook(aws_conn_id="aws_default").get_conn()
        save_all_to_s3(results, s3_client=s3_client)
        return results

    @task
    def load_bronze(results: dict):
        """Step 4: insert into RAW.RAW_NEWS, skipping 10hr duplicates."""
        conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
        save_all_to_snowflake(results, conn=conn)
        return results

    @task
    def load_silver(results: dict):
        """Step 5: dedup by title hash across sources, insert into STAGING.STAGED_NEWS."""
        conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
        bronze_to_silver(results, conn=conn)

    @task
    def enrich_with_llm():
        """Step 6: pull up to 3 unprocessed silver rows, get summary + sentiment from Gemini."""
        conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
        api_key = Variable.get("gemini_api_key")
        enrich_staged_news(conn=conn, api_key=api_key)

    # --- wire the tasks together, same order as run.py ---
    fetched    = fetch_rss()
    scraped    = scrape_articles(fetched)
    landed     = save_to_s3(scraped)
    bronze     = load_bronze(landed)
    silver     = load_silver(bronze)
    enrichment = enrich_with_llm()

    # load_silver doesn't need enrich_with_llm's output, so this edge has
    # to be declared explicitly - it won't happen automatically like the
    # data-passing dependencies above.
    silver >> enrichment


news_ai_etl()
