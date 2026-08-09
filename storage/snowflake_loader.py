import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()


SNOWFLAKE_CONFIG = {
    "account":   "kneiclg-bub47449",
    "user":      "TAHAKHAN",
    "password":  os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": "NEWS_WH",
    "database":  "NEWS_AI_ETL",
    "schema":    "RAW",
    "role":      "ACCOUNTADMIN",
}


def get_connection():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    return conn


def get_recent_links(cursor, source):
    """
    Fetch all links already saved in the last 10 hours for this source.
    We use this to skip articles we already have — no need to check older data
    because RSS feeds refresh their articles within hours anyway.
    """

    query = """
        SELECT link
        FROM RAW_NEWS
        WHERE source = %s
        AND TRY_TO_TIMESTAMP(fetched_at) >= DATEADD(hour, -10, CURRENT_TIMESTAMP())
    """

    cursor.execute(query, (source,))
    rows = cursor.fetchall()

    # Return a set of links for fast lookup
    return set(row[0] for row in rows)


def filter_new_articles(articles, existing_links):
    """
    Remove articles whose link already exists in Snowflake (last 10 hrs).
    Return only the new ones.
    """

    new_articles     = [a for a in articles if a["link"] not in existing_links]
    skipped_count    = len(articles) - len(new_articles)

    if skipped_count > 0:
        print(f"  Skipped {skipped_count} duplicates (already in Snowflake)")

    return new_articles


def insert_articles(cursor, articles):
    """
    Insert a list of articles into RAW.RAW_NEWS table using the given cursor.
    Skips articles already fetched in the last 10 hours (Scenario 1 dedup).

    Does not commit or close anything - the caller (save_all_to_snowflake)
    owns the connection lifecycle so it can be reused across sources and
    shared with an Airflow-provided connection.
    """

    # --- Dedup: check what we already have in the last 10 hours ---
    source         = articles[0]["source"] if articles else ""
    existing_links = get_recent_links(cursor, source)
    articles       = filter_new_articles(articles, existing_links)

    if not articles:
        print(f"  Nothing new to insert for {source}")
        return

    # --- Insert only new articles ---
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

    print(f"  Inserted {len(rows)} new articles into RAW.RAW_NEWS")


def save_all_to_snowflake(results, conn=None):
    """
    results is a dict: { "yahoo_finance": [...], "cnbc_markets": [...] }
    Loop through each source and insert into Snowflake.

    conn is optional - pass one in (e.g. from Airflow's SnowflakeHook) to
    reuse an existing connection. Defaults to get_connection() (.env-based)
    for local runs. A connection we open ourselves is also closed by us;
    one passed in is left for the caller to manage.
    """

    owns_connection = conn is None
    conn   = conn or get_connection()
    cursor = conn.cursor()

    for source_name, articles in results.items():
        print(f"\nLoading {source_name} into Snowflake ...")
        insert_articles(cursor, articles)

    conn.commit()
    cursor.close()

    if owns_connection:
        conn.close()
