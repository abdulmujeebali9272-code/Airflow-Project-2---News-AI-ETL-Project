-- ============================================================
-- Run this once in Snowflake to set up the raw layer
-- ============================================================

-- Step 1: Create the database
CREATE DATABASE IF NOT EXISTS NEWS_AI_ETL;

-- Step 2: Create schemas (RAW = bronze layer for now)
CREATE SCHEMA IF NOT EXISTS NEWS_AI_ETL.RAW;

-- Step 3: Create a warehouse to run queries
CREATE WAREHOUSE IF NOT EXISTS NEWS_WH
    WAREHOUSE_SIZE  = 'XSMALL'
    AUTO_SUSPEND    = 60
    AUTO_RESUME     = TRUE;

-- Step 4: Create the raw news table
CREATE TABLE IF NOT EXISTS NEWS_AI_ETL.RAW.RAW_NEWS (
    source       VARCHAR(100),    -- which feed: yahoo_finance, cnbc, etc.
    title        VARCHAR(1000),   -- article headline
    link         VARCHAR(2000),   -- article URL
    published    VARCHAR(200),    -- date string from RSS feed
    description  TEXT,            -- opening lines of the article from RSS feed
    text         TEXT,            -- full article body (scraped)
    fetched_at   VARCHAR(50)      -- when we fetched it
);
