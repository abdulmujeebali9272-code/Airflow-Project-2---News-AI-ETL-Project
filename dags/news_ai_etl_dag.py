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

from airflow.decorators import dag, task, task_group
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

    # ------------------------------------------------------------------
    # Ingestion: RSS Sources -> rss_fetcher.py -> scraper.py
    # ------------------------------------------------------------------
    @task_group(group_id="ingestion")
    def ingestion():
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

        return scrape_articles(fetch_rss())

    # ------------------------------------------------------------------
    # Bronze — S3 + Snowflake RAW: land raw data, dedup 10hr window
    # ------------------------------------------------------------------
    @task_group(group_id="bronze")
    def bronze(results: dict):
        @task
        def save_to_s3(results: dict):
            """S3 Bucket: news-ai-etl-raw/raw/source/year/month/day/"""
            s3_client = S3Hook(aws_conn_id="aws_default").get_conn()
            save_all_to_s3(results, s3_client=s3_client)
            return results

        @task
        def load_bronze(results: dict):
            """RAW.RAW_NEWS - dedup: 10hr window by source + URL."""
            conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
            save_all_to_snowflake(results, conn=conn)
            return results

        return load_bronze(save_to_s3(results))

    # ------------------------------------------------------------------
    # Silver — Snowflake STAGING: title-hash dedup + LLM summary/sentiment
    # ------------------------------------------------------------------
    @task_group(group_id="silver")
    def silver(results: dict):
        @task
        def load_silver(results: dict):
            """Title Hash Dedup -> STAGING.STAGED_NEWS (title_hash, summary, sentiment)."""
            conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
            bronze_to_silver(results, conn=conn)

        @task
        def enrich_with_llm():
            """LLM - Summary + Sentiment: up to 3 unprocessed silver rows via Gemini."""
            conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
            api_key = Variable.get("gemini_api_key")
            enrich_staged_news(conn=conn, api_key=api_key)

        loaded = load_silver(results)
        enriched = enrich_with_llm()
        loaded >> enriched

    # --- wire the groups together, same order as run.py ---
    ingested = ingestion()
    landed = bronze(ingested)
    silver(landed)


news_ai_etl()