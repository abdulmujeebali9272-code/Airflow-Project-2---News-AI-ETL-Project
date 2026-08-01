import hashlib
from datetime import datetime
from storage.snowflake_loader import get_connection


def make_title_hash(title):
    clean_title = title.lower().strip()
    return hashlib.md5(clean_title.encode()).hexdigest()


def get_existing_hashes(cursor):
    """
    Fetch all title hashes already in STAGED_NEWS so we can skip duplicates.
    """
    cursor.execute("SELECT title_hash FROM NEWS_AI_ETL.STAGING.STAGED_NEWS")
    rows = cursor.fetchall()
    return set(row[0] for row in rows)


def bronze_to_silver(results):
    """
    Takes the current run's articles (already inserted into bronze),
    deduplicates by title hash, and inserts unique stories into silver.

    results = { "yahoo_finance": [...], "cnbc_markets": [...], ... }
    """

    conn   = get_connection()
    cursor = conn.cursor()

    # Flatten all articles from all sources into one list
    all_articles = []
    for articles in results.values():
        all_articles.extend(articles)

    print(f"  Current run has {len(all_articles)} articles across all sources")

    print("  Fetching existing title hashes from silver ...")
    existing_hashes = get_existing_hashes(cursor)
    print(f"  {len(existing_hashes)} stories already in silver")

    insert_sql = """
        INSERT INTO NEWS_AI_ETL.STAGING.STAGED_NEWS
            (title_hash, source, title, link, published, description, text, fetched_at, staged_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    inserted = 0
    skipped  = 0

    for article in all_articles:
        title_hash = make_title_hash(article["title"])

        if title_hash in existing_hashes:
            skipped += 1
            continue

        cursor.execute(insert_sql, (
            title_hash,
            article.get("source", ""),
            article.get("title", ""),
            article.get("link", ""),
            article.get("published", ""),
            article.get("description", ""),
            article.get("text", ""),
            article.get("fetched_at", ""),
            datetime.now().isoformat(),
        ))

        existing_hashes.add(title_hash)
        inserted += 1

    conn.commit()

    print(f"  Inserted : {inserted} unique stories into silver")
    print(f"  Skipped  : {skipped} duplicates (same story seen in multiple sources or already staged)")

    cursor.close()
    conn.close()
