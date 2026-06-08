-- ============================================================
-- Snowflake Setup — Run this in Snowflake UI (Worksheets)
-- ============================================================
-- Creates: warehouse, database, schemas, roles, Bronze tables
-- Run as ACCOUNTADMIN or SYSADMIN
-- ============================================================

-- ── Warehouse ───────────────────────────────────────────────
CREATE WAREHOUSE IF NOT EXISTS BANKING_WH
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- ── Database ────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS BANKING_DW;

-- ── Schemas (Medallion Architecture) ────────────────────────
CREATE SCHEMA IF NOT EXISTS BANKING_DW.BRONZE;
CREATE SCHEMA IF NOT EXISTS BANKING_DW.SILVER;
CREATE SCHEMA IF NOT EXISTS BANKING_DW.GOLD;
CREATE SCHEMA IF NOT EXISTS BANKING_DW.CI_TEST;   -- for CI/CD runs

-- ── Service Role ────────────────────────────────────────────
CREATE ROLE IF NOT EXISTS BANKING_TRANSFORMER;
GRANT USAGE ON WAREHOUSE BANKING_WH TO ROLE BANKING_TRANSFORMER;
GRANT ALL ON DATABASE BANKING_DW TO ROLE BANKING_TRANSFORMER;
GRANT ALL ON ALL SCHEMAS IN DATABASE BANKING_DW TO ROLE BANKING_TRANSFORMER;
GRANT ALL ON FUTURE TABLES IN DATABASE BANKING_DW TO ROLE BANKING_TRANSFORMER;
GRANT ALL ON FUTURE VIEWS IN DATABASE BANKING_DW TO ROLE BANKING_TRANSFORMER;

-- Grant role to your user
-- GRANT ROLE BANKING_TRANSFORMER TO USER <your_username>;

-- ── Bronze Landing Tables ───────────────────────────────────
-- These store raw Debezium CDC payloads as VARIANT (semi-structured JSON).
-- dbt staging models will parse and type-cast from these.

USE SCHEMA BANKING_DW.BRONZE;

CREATE TABLE IF NOT EXISTS RAW_CUSTOMERS (
    raw_data        VARIANT,
    source_file     VARCHAR(500),
    loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW_ACCOUNTS (
    raw_data        VARIANT,
    source_file     VARCHAR(500),
    loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW_TRANSACTIONS (
    raw_data        VARIANT,
    source_file     VARCHAR(500),
    loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW_LOANS (
    raw_data        VARIANT,
    source_file     VARCHAR(500),
    loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW_AUDIT_LOG (
    raw_data        VARIANT,
    source_file     VARCHAR(500),
    loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ── Verify Setup ────────────────────────────────────────────
SELECT 'Setup complete' AS status;
SHOW SCHEMAS IN DATABASE BANKING_DW;
SHOW TABLES IN SCHEMA BANKING_DW.BRONZE;
