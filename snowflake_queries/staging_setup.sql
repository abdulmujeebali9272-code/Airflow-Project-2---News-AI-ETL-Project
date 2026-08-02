-- ============================================================
-- Run this once to set up the Silver / Staging layer
-- ============================================================

CREATE SCHEMA IF NOT EXISTS NEWS_AI_ETL.STAGING;

CREATE TABLE IF NOT EXISTS NEWS_AI_ETL.STAGING.STAGED_NEWS (
    title_hash   VARCHAR(32),    -- MD5 of normalized title — our dedup key
    source       VARCHAR(100),   -- first source that had this story
    title        VARCHAR(1000),
    link         VARCHAR(2000),
    published    VARCHAR(200),
    description  TEXT,
    text         TEXT,
    fetched_at   VARCHAR(50),
    staged_at    VARCHAR(50)
);

-- ============================================================
-- LLM enrichment columns (Gemini summary + sentiment)
-- ============================================================
ALTER TABLE NEWS_AI_ETL.STAGING.STAGED_NEWS ADD COLUMN IF NOT EXISTS summary TEXT;         -- LLM-generated summary
ALTER TABLE NEWS_AI_ETL.STAGING.STAGED_NEWS ADD COLUMN IF NOT EXISTS sentiment VARCHAR(20); -- positive / negative / neutral
ALTER TABLE NEWS_AI_ETL.STAGING.STAGED_NEWS ADD COLUMN IF NOT EXISTS enriched_at VARCHAR(50); -- when the LLM step ran
