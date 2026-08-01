from ingestion.rss_fetcher import fetch_all
from ingestion.scraper import add_text_to_articles
from storage.s3_storage import save_all_to_s3
from storage.snowflake_loader import save_all_to_snowflake


def main():
    print("=== Step 1: Fetching RSS Feeds ===")
    results = fetch_all()

    print("\n=== Step 2: Scraping Article Text ===")
    for source, articles in results.items():
        print(f"\nScraping {source} ...")
        results[source] = add_text_to_articles(articles)

    print("\n=== Step 3: Saving Raw JSON to S3 ===")
    save_all_to_s3(results)

    print("\n=== Step 4: Loading Raw Data into Snowflake ===")
    save_all_to_snowflake(results)

    print("\nDone!")


if __name__ == "__main__":
    main()
