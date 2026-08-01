import feedparser
from datetime import datetime
from ingestion.config import RSS_FEEDS


def fetch_feed(name, url):
    """
    Fetch one RSS feed and return a list of articles.
    Each article is a simple dictionary with the fields we care about.
    """

    print(f"Fetching: {name} ...")

    # feedparser reads the RSS URL and parses the XML for us
    feed = feedparser.parse(url)

    articles = []

    for entry in feed.entries:

        article = {
            "source":     name,
            "title":      entry.get("title", ""),
            "link":       entry.get("link", ""),
            "published":  entry.get("published", ""),
            "description": entry.get("summary", ""),
            "fetched_at": datetime.now().isoformat(),
        }

        articles.append(article)

    print(f"  -> Got {len(articles)} articles")
    return articles


def fetch_all():
    """
    Loop through every feed in config and fetch all of them.
    Returns a dictionary:  { feed_name: [list of articles] }
    """

    all_results = {}

    for feed in RSS_FEEDS:
        articles = fetch_feed(feed["name"], feed["url"])
        all_results[feed["name"]] = articles

    return all_results


# Run this file directly to test
if __name__ == "__main__":

    results = fetch_all()

    # Print a quick preview
    for source, articles in results.items():
        print(f"\n=== {source} ({len(articles)} articles) ===")
        for article in articles[:3]:           # show first 3 only
            print(f"  Title : {article['title']}")
            print(f"  Link  : {article['link']}")
            print(f"  Date  : {article['published']}")
            print()
