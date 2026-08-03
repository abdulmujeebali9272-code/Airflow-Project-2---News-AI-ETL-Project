# News AI ETL — Architecture Diagram

![Architecture Diagram](ArchitectureDiagram.png)

```mermaid
flowchart LR

    subgraph SRC["RSS Sources"]
        direction TB
        Y["Yahoo Finance"]
        C["CNBC Markets"]
        MW["MarketWatch"]
    end

    subgraph ING["Ingestion"]
        direction TB
        F["rss_fetcher.py - Fetch articles"]
        SC["scraper.py - Scrape full text"]
    end

    S3["S3 Bucket: news-ai-etl-raw\nraw/source/year/month/day/"]

    subgraph BRONZE["Bronze — Snowflake RAW"]
        B["RAW.RAW_NEWS\nDedup: 10hr window by source + URL"]
    end

    subgraph SILVER["Silver — Snowflake STAGING"]
        direction TB
        D["Title Hash Dedup - Scenario 2 filter"]
        LLM["Gemini - Summary + Sentiment\n(3 rows per run)"]
        SN["STAGED_NEWS\ntitle_hash · summary · sentiment"]
    end

    Y --> F
    C --> F
    MW --> F
    F --> SC
    SC --> S3
    SC --> B
    B --> D
    D --> SN
    SN --> LLM
    LLM --> SN
```

## Orchestration

`dags/news_ai_etl_dag.py` runs this same flow as six Airflow tasks
(`fetch_rss` → `scrape_articles` → `save_to_s3` → `load_bronze` →
`load_silver` → `enrich_with_llm`), scheduled every 6 hours. Credentials
come from Airflow Connections (`aws_default`, `snowflake_default`) and an
Airflow Variable (`gemini_api_key`) rather than `.env` — see the
"Airflow Setup" section in the main [README](../README.md) for exact steps.
