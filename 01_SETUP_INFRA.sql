-- =========================================================
-- 1. CREATE WAREHOUSES
-- =========================================================

CREATE WAREHOUSE IF NOT EXISTS FRAUD_ETL_WH
WITH 
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_RESUME = TRUE
    AUTO_SUSPEND = 80;

CREATE WAREHOUSE IF NOT EXISTS FRAUD_ANALYTICS_WH
WITH 
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_RESUME = TRUE
    AUTO_SUSPEND = 80;


-- =========================================================
-- 2. CREATE DATABASE
-- =========================================================

CREATE DATABASE IF NOT EXISTS FRAUD_DETECTION_DB;


-- =========================================================
-- 3. USE DATABASE
-- =========================================================

USE DATABASE FRAUD_DETECTION_DB;


-- =========================================================
-- 4. CREATE SCHEMAS (LAYERED ARCHITECTURE)
-- =========================================================

CREATE SCHEMA IF NOT EXISTS FRAUD_DETECTION_DB.RAW;
CREATE SCHEMA IF NOT EXISTS FRAUD_DETECTION_DB.STAGING;
CREATE SCHEMA IF NOT EXISTS FRAUD_DETECTION_DB.CORE;
CREATE SCHEMA IF NOT EXISTS FRAUD_DETECTION_DB.PIPELINE;
CREATE SCHEMA IF NOT EXISTS FRAUD_DETECTION_DB.ANALYTICS;
CREATE SCHEMA IF NOT EXISTS FRAUD_DETECTION_DB.SECURITY;


-- =========================================================
-- 5. SET DEFAULT CONTEXT (OPTIONAL BUT RECOMMENDED)
-- =========================================================

USE WAREHOUSE FRAUD_ETL_WH;
USE SCHEMA FRAUD_DETECTION_DB.RAW;