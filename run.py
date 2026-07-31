from ingestion.rss_fetcher import fetch_all
from storage.s3_storage import save_all_to_s3


def main():
    print("=== Step 1: Fetching RSS Feeds ===")
    results = fetch_all()

    print("\n=== Step 2: Saving to S3 ===")
    save_all_to_s3(results)

    print("\nDone!")


if __name__ == "__main__":
    main()
