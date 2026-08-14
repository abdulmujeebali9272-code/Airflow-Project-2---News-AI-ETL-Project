"""
Airflow DAG version of run.py.

Structure mirrors the architecture diagram:

    Yahoo Finance  --+
    CNBC Markets   --+--> scrape_articles --+--> save_to_s3   (S3 raw landing)
    MarketWatch    --+                      |
                                            +--> bronze --> silver

The three RSS feeds are polled in parallel, merged by the scraper, then the
S3 landing and the bronze Snowflake load fan out in parallel since neither
depends on the other's output.

Credentials come from Airflow instead of .env:
  - S3        -> Airflow Connection "aws_default"        (Admin > Connections)
  - Snowflake -> Airflow Connection "snowflake_default"  (Admin > Connections)
  - Gemini    -> Airflow Variable   "gemini_api_key"     (Admin > Variables)

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
    "owner": "Mujeeb",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def fetch_one(source_key: str) -> dict:
    """
    Poll the feeds and return only the requested source's articles.

    NOTE: fetch_all() hits all three feeds every call, so with one task per
    source each feed gets polled three times. RSS payloads are small enough
    that this is fine, but if rss_fetcher.py exposes a single-feed function
    it should be imported and called here instead.
    """
    all_results = fetch_all()
    if source_key not in all_results:
        raise KeyError(
            f"Source '{source_key}' not found in fetch_all() output. "
            f"Available keys: {list(all_results.keys())}"
        )
    return {source_key: all_results[source_key]}


@dag(
    dag_id="news_ai_etl",
    description="Fetch financial news, scrape full text, land in S3 + Snowflake bronze/silver, enrich with Gemini",
    schedule="0 */6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["news", "etl", "snowflake", "llm"],
)
def news_ai_etl():

    # ------------------------------------------------------------------
    # RSS Sources - one task per feed, all run in parallel
    # ------------------------------------------------------------------
    @task_group(group_id="rss_sources")
    def rss_sources():
        @task
        def yahoo_finance():
            """Poll the Yahoo Finance RSS feed."""
            return fetch_one("yahoo_finance")

        @task
        def cnbc_markets():
            """Poll the CNBC Markets RSS feed."""
            return fetch_one("cnbc_markets")

        @task
        def marketwatch():
            """Poll the MarketWatch RSS feed."""
            return fetch_one("marketwatch")

        return [yahoo_finance(), cnbc_markets(), marketwatch()]

    # ------------------------------------------------------------------
    # Ingestion - merge the three feeds, then scrape full article text
    # ------------------------------------------------------------------
    @task_group(group_id="ingestion")
    def ingestion(feeds: list):
        @task
        def scrape_articles(feeds: list):
            """Merge the per-source dicts, then visit each link for body text."""
            results = {}
            for feed in feeds:
                results.update(feed)

            for source, articles in results.items():
                results[source] = add_text_to_articles(articles)
            return results

        return scrape_articles(feeds)

    # ------------------------------------------------------------------
    # S3 raw landing - parallel branch, nothing downstream depends on it
    # ------------------------------------------------------------------
    @task
    def save_to_s3(results: dict):
        """S3 Bucket: news-ai-etl-raw/raw/source/year/month/day/"""
        s3_client = S3Hook(aws_conn_id="aws_default").get_conn()
        save_all_to_s3(results, s3_client=s3_client)

    # ------------------------------------------------------------------
    # Bronze - Snowflake RAW: land raw data, dedup 10hr window
    # ------------------------------------------------------------------
    @task_group(group_id="bronze")
    def bronze(results: dict):
        @task
        def load_bronze(results: dict):
            """RAW.RAW_NEWS - dedup: 10hr window by source + URL."""
            conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
            save_all_to_snowflake(results, conn=conn)
            return results

        return load_bronze(results)

    # ------------------------------------------------------------------
    # Silver - Snowflake STAGING: title-hash dedup + LLM summary/sentiment
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

        load_silver(results) >> enrich_with_llm()

    # ------------------------------------------------------------------
    # Wiring: 3 feeds -> scraper -> fan out to S3 and bronze
    # ------------------------------------------------------------------
    feeds = rss_sources()
    ingested = ingestion(feeds)

    save_to_s3(ingested)          # parallel branch
    silver(bronze(ingested))      # main medallion path


news_ai_etl()