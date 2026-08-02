import os
import json
from datetime import datetime
from google import genai
from google.genai import types
from dotenv import load_dotenv
from storage.snowflake_loader import get_connection

load_dotenv()

MODEL_NAME = "gemini-flash-latest"
MAX_ROWS   = 3
MAX_CHARS  = 6000

# ------------------------------------------------------------------
# A Gemini call has three parts — same idea as any chat-based LLM API:
#
#   1. SYSTEM PROMPT  -> "bata do woh kaun hai"   (defines the role + rules)
#   2. USER PROMPT    -> "actual kaam do"          (the real task: the article)
#   3. RESPONSE       -> structured answer         (JSON, enforced below)
# ------------------------------------------------------------------

SYSTEM_PROMPT = """You are a financial news analyst.
Read the article the user gives you and produce:
- a 2-3 sentence summary
- an overall sentiment: "positive", "negative", or "neutral"
Always respond in the JSON schema you were given. No extra commentary."""

USER_PROMPT_TEMPLATE = """Article title: {title}

Article text:
{text}
"""

RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "summary":   {"type": "string"},
        "sentiment": {"type": "string", "enum": ["positive", "negative", "neutral"]},
    },
    "required": ["summary", "sentiment"],
}


def get_client(api_key=None):
    """
    Local/CLI Gemini client - reads the key from .env by default.
    api_key is optional - pass one in (e.g. from an Airflow Variable) to
    avoid relying on the .env file when running inside Airflow.
    Built lazily (not at import time) so Airflow can supply the key
    before this is ever called.
    """
    return genai.Client(api_key=api_key or os.getenv("GEMINI_API_KEY"))


def get_unprocessed_articles(cursor, limit=MAX_ROWS):
    """
    Fetch the newest staged articles that haven't been enriched yet.
    """
    query = """
        SELECT title_hash, title, text
        FROM NEWS_AI_ETL.STAGING.STAGED_NEWS
        WHERE summary IS NULL
        AND text IS NOT NULL
        AND text != ''
        ORDER BY staged_at DESC
        LIMIT %s
    """
    cursor.execute(query, (limit,))
    return cursor.fetchall()


def analyze_article(title, text, client=None):
    """
    Send the article to Gemini and get back a summary + sentiment.
    Returns (summary, sentiment) or (None, None) if anything goes wrong.
    """
    client = client or get_client()
    trimmed = text[:MAX_CHARS]
    user_prompt = USER_PROMPT_TEMPLATE.format(title=title, text=trimmed)

    try:
        response = client.models.generate_content(
            model=MODEL_NAME,
            contents=user_prompt,
            config=types.GenerateContentConfig(
                system_instruction=SYSTEM_PROMPT,
                response_mime_type="application/json",
                response_schema=RESPONSE_SCHEMA,
            ),
        )

        data = json.loads(response.text)
        return data.get("summary", "").strip(), data.get("sentiment", "").strip().lower()

    except Exception as e:
        print(f"    Error analyzing article: {e}")
        return None, None


def update_article(cursor, title_hash, summary, sentiment):
    update_sql = """
        UPDATE NEWS_AI_ETL.STAGING.STAGED_NEWS
        SET summary = %s, sentiment = %s, enriched_at = %s
        WHERE title_hash = %s
    """
    cursor.execute(update_sql, (summary, sentiment, datetime.now().isoformat(), title_hash))


def enrich_staged_news(conn=None, api_key=None):
    """
    Pull up to MAX_ROWS unprocessed articles from silver, run them through
    Gemini for a summary + sentiment, and write the results back.

    conn is optional - pass one in (e.g. from Airflow's SnowflakeHook) to
    reuse an existing connection. Defaults to get_connection() (.env-based)
    for local runs. A connection we open ourselves is also closed by us;
    one passed in is left for the caller to manage.

    api_key is optional - pass one in (e.g. from an Airflow Variable)
    instead of relying on the .env file.
    """
    owns_connection = conn is None
    conn   = conn or get_connection()
    cursor = conn.cursor()
    client = get_client(api_key)

    articles = get_unprocessed_articles(cursor)
    print(f"  Found {len(articles)} unprocessed article(s) (limit {MAX_ROWS})")

    enriched = 0

    for title_hash, title, text in articles:
        print(f"  Analyzing: {title[:60]}...")
        summary, sentiment = analyze_article(title, text, client=client)

        if summary is None:
            print("    Skipped (LLM error)")
            continue

        update_article(cursor, title_hash, summary, sentiment)
        enriched += 1

    conn.commit()

    print(f"  Enriched {enriched}/{len(articles)} article(s) with summary + sentiment")

    cursor.close()

    if owns_connection:
        conn.close()
