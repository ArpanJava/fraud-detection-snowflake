-- =========================================================
-- 1. SET CONTEXT
-- =========================================================

USE WAREHOUSE FRAUD_ETL_WH;
USE DATABASE FRAUD_DETECTION_DB;
USE SCHEMA STAGING;


-- =========================================================
-- 2. CREATE STAGING TABLES
-- =========================================================

-- Cleaned Transactions Table
CREATE OR REPLACE TABLE STG_TRANSACTIONS (
    transaction_id STRING DEFAULT UUID_STRING(),
    customer_id INTEGER,
    transaction_time TIMESTAMP,
    amount FLOAT,
    fraud_flag INTEGER,

    -- PCA Features
    v1 FLOAT, v2 FLOAT, v3 FLOAT, v4 FLOAT, v5 FLOAT, v6 FLOAT,
    v7 FLOAT, v8 FLOAT, v9 FLOAT, v10 FLOAT, v11 FLOAT, v12 FLOAT,
    v13 FLOAT, v14 FLOAT, v15 FLOAT, v16 FLOAT, v17 FLOAT, v18 FLOAT,
    v19 FLOAT, v20 FLOAT, v21 FLOAT, v22 FLOAT, v23 FLOAT, v24 FLOAT,
    v25 FLOAT, v26 FLOAT, v27 FLOAT, v28 FLOAT,

    created_at TIMESTAMP,
    updated_at TIMESTAMP
);


-- Cleaned Customers Table
CREATE OR REPLACE TABLE STG_CUSTOMERS (
    customer_id INTEGER,
    customer_name STRING,
    email STRING,
    phone_number STRING,
    address STRING,
    created_at TIMESTAMP,
    is_active BOOLEAN
);


-- =========================================================
-- 3. LOAD DATA: RAW → STAGING (TRANSACTIONS)
-- =========================================================

INSERT INTO STG_TRANSACTIONS (
    transaction_id,
    customer_id,
    transaction_time,
    amount,
    fraud_flag,
    v1,v2,v3,v4,v5,v6,
    v7,v8,v9,v10,v11,v12,
    v13,v14,v15,v16,v17,v18,
    v19,v20,v21,v22,v23,v24,
    v25,v26,v27,v28,
    created_at,
    updated_at
)
SELECT
    UUID_STRING() AS transaction_id,

    -- Simulated customer mapping (replace later with real mapping)
    UNIFORM(1, 1001, RANDOM()) AS customer_id,

    -- FIXED timestamp logic (important)
    DATEADD(SECOND, raw_payload:TIME::NUMBER, '2024-01-01'::TIMESTAMP),

    raw_payload:AMOUNT::FLOAT,
    raw_payload:CLASS::INTEGER,

    raw_payload:V1::FLOAT,
    raw_payload:V2::FLOAT,
    raw_payload:V3::FLOAT,
    raw_payload:V4::FLOAT,
    raw_payload:V5::FLOAT,
    raw_payload:V6::FLOAT,
    raw_payload:V7::FLOAT,
    raw_payload:V8::FLOAT,
    raw_payload:V9::FLOAT,
    raw_payload:V10::FLOAT,
    raw_payload:V11::FLOAT,
    raw_payload:V12::FLOAT,
    raw_payload:V13::FLOAT,
    raw_payload:V14::FLOAT,
    raw_payload:V15::FLOAT,
    raw_payload:V16::FLOAT,
    raw_payload:V17::FLOAT,
    raw_payload:V18::FLOAT,
    raw_payload:V19::FLOAT,
    raw_payload:V20::FLOAT,
    raw_payload:V21::FLOAT,
    raw_payload:V22::FLOAT,
    raw_payload:V23::FLOAT,
    raw_payload:V24::FLOAT,
    raw_payload:V25::FLOAT,
    raw_payload:V26::FLOAT,
    raw_payload:V27::FLOAT,
    raw_payload:V28::FLOAT,

    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()

FROM FRAUD_DETECTION_DB.RAW.RAW_TRANSACTIONS;


-- =========================================================
-- 4. LOAD DATA: RAW → STAGING (CUSTOMERS)
-- =========================================================

INSERT INTO STG_CUSTOMERS (
    customer_id,
    customer_name,
    email,
    phone_number,
    address,
    created_at,
    is_active
)
WITH CUSTOMER_DATA AS (
    SELECT
        customer_id,
        UPPER(TRIM(customer_name)) AS customer_name,
        LOWER(TRIM(email)) AS email,
        TRIM(phone_number) AS phone_number,
        UPPER(TRIM(address)) AS address,
        created_at
    FROM FRAUD_DETECTION_DB.RAW.RAW_CUSTOMERS
)
SELECT
    customer_id,
    customer_name,
    email,
    phone_number,
    address,
    created_at,
    TRUE
FROM CUSTOMER_DATA;


-- =========================================================
-- 5. DATA VALIDATION
-- =========================================================

-- Row count check
SELECT COUNT(*) FROM STG_TRANSACTIONS;

-- Sample data
SELECT amount, fraud_flag, transaction_time
FROM STG_TRANSACTIONS
LIMIT 10;

-- Check for nulls
SELECT *
FROM STG_TRANSACTIONS
WHERE amount IS NULL OR transaction_time IS NULL;

-- Customer data check
SELECT * FROM STG_CUSTOMERS LIMIT 10;