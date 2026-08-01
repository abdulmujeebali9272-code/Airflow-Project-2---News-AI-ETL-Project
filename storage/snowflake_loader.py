import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()


# Snowflake connection config
# Password is read from environment variable — never hardcode it
SNOWFLAKE_CONFIG = {
    "account":   "XBYCFDM-CO68158",
    "user":      "AYANHUSSAIN",
    "password":  os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": "NEWS_WH",
    "database":  "NEWS_AI_ETL",
    "schema":    "RAW",
    "role":      "ACCOUNTADMIN",
}


def get_connection():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    return conn


def insert_articles(articles):
    """
    Insert a list of articles into RAW.RAW_NEWS table.
    Each article is a dict with: source, title, link, published, summary, text, fetched_at
    """

    conn   = get_connection()
    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO RAW_NEWS (source, title, link, published, description, text, fetched_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    rows = []
    for article in articles:
        row = (
            article.get("source", ""),
            article.get("title", ""),
            article.get("link", ""),
            article.get("published", ""),
            article.get("description", ""),
            article.get("text", ""),
            article.get("fetched_at", ""),
        )
        rows.append(row)

    cursor.executemany(insert_sql, rows)
    conn.commit()

    print(f"  Inserted {len(rows)} rows into RAW.RAW_NEWS")

    cursor.close()
    conn.close()


def save_all_to_snowflake(results):
    """
    results is a dict: { "yahoo_finance": [...], "cnbc_markets": [...] }
    Loop through each source and insert into Snowflake.
    """

    for source_name, articles in results.items():
        print(f"\nLoading {source_name} into Snowflake ...")
        insert_articles(articles)
