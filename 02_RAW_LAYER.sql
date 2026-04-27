-- =========================================================
-- 1. SET CONTEXT
-- =========================================================

USE WAREHOUSE FRAUD_ETL_WH;
USE DATABASE FRAUD_DETECTION_DB;
USE SCHEMA RAW;


-- =========================================================
-- 2. CREATE RAW TABLES (LANDING ZONE)
-- =========================================================

-- Raw Customers Table
CREATE OR REPLACE TABLE RAW_CUSTOMERS (
    customer_id INTEGER,
    customer_name STRING,
    email STRING,
    phone_number STRING,
    address STRING,
    created_at TIMESTAMP
);

-- Raw Transactions (Semi-structured)
CREATE OR REPLACE TABLE RAW_TRANSACTIONS (
    raw_payload VARIANT,
    ingestion_timestamp TIMESTAMP
);

-- Raw Devices (for future use)
CREATE OR REPLACE TABLE RAW_DEVICES (
    device_id STRING,
    customer_id INTEGER,
    device_type STRING,
    ip_address STRING,
    created_at TIMESTAMP
);


-- =========================================================
-- 3. CREATE STAGE (EXTERNAL/INTERNAL DATA SOURCE)
-- =========================================================

CREATE OR REPLACE STAGE FRAUD_DETECTION_DB.RAW.RAW_DATA;


-- =========================================================
-- 4. CREATE TEMP TABLE FOR FILE INGESTION
-- =========================================================

CREATE OR REPLACE TEMPORARY TABLE TEMP_RAW_TRANSACTIONS (
    Time NUMBER,
    v1 FLOAT, v2 FLOAT, v3 FLOAT, v4 FLOAT, v5 FLOAT, v6 FLOAT,
    v7 FLOAT, v8 FLOAT, v9 FLOAT, v10 FLOAT, v11 FLOAT, v12 FLOAT,
    v13 FLOAT, v14 FLOAT, v15 FLOAT, v16 FLOAT, v17 FLOAT, v18 FLOAT,
    v19 FLOAT, v20 FLOAT, v21 FLOAT, v22 FLOAT, v23 FLOAT, v24 FLOAT,
    v25 FLOAT, v26 FLOAT, v27 FLOAT, v28 FLOAT,
    Amount FLOAT,
    class INTEGER
);


-- =========================================================
-- 5. LOAD DATA FROM STAGE → TEMP TABLE
-- =========================================================

-- Check files in stage
LIST @FRAUD_DETECTION_DB.RAW.RAW_DATA;

-- Load CSV into temp table
COPY INTO TEMP_RAW_TRANSACTIONS
FROM @FRAUD_DETECTION_DB.RAW.RAW_DATA
FILE_FORMAT = (
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
);


-- =========================================================
-- 6. LOAD DATA INTO RAW TRANSACTIONS (VARIANT)
-- =========================================================

INSERT INTO RAW_TRANSACTIONS (
    raw_payload,
    ingestion_timestamp
)
SELECT
    OBJECT_CONSTRUCT(*) AS raw_payload,
    CURRENT_TIMESTAMP()
FROM TEMP_RAW_TRANSACTIONS;


-- =========================================================
-- 7. VALIDATION QUERIES
-- =========================================================

-- Check sample raw data
SELECT raw_payload
FROM RAW_TRANSACTIONS
LIMIT 5;

-- Check fraud records
SELECT 
    raw_payload:TIME AS time,
    raw_payload:AMOUNT AS amount,
    raw_payload:CLASS AS fraud_flag
FROM RAW_TRANSACTIONS
WHERE raw_payload:CLASS = 1
LIMIT 5;