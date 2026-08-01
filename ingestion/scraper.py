import requests
from bs4 import BeautifulSoup


# Some websites block requests that don't have a browser user-agent
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36"
}


def scrape_article_text(url):
    """
    Given a news article URL, fetch the page and extract all paragraph text.
    Returns the full text as a single string.
    Returns empty string if anything goes wrong.
    """

    try:
        response = requests.get(url, headers=HEADERS, timeout=10)

        if response.status_code != 200:
            print(f"    Could not fetch {url} (status {response.status_code})")
            return ""

        soup = BeautifulSoup(response.text, "html.parser")

        # Grab all <p> tags — this is where article body text lives
        paragraphs = soup.find_all("p")

        # Join all paragraph texts into one block
        text = " ".join(p.get_text() for p in paragraphs)

        return text.strip()

    except Exception as e:
        print(f"    Error scraping {url}: {e}")
        return ""


def add_text_to_articles(articles):
    """
    Loop through articles and add a 'text' field to each one
    by scraping the article URL.
    """

    for i, article in enumerate(articles):
        print(f"  Scraping article {i + 1}/{len(articles)}: {article['title'][:60]}...")
        article["text"] = scrape_article_text(article["link"])

    return articles
