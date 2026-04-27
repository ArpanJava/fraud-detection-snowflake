-- =========================================================
-- 1. SET CONTEXT
-- =========================================================

USE WAREHOUSE FRAUD_ANALYTICS_WH;
USE DATABASE FRAUD_DETECTION_DB;


-- =========================================================
-- 2. ROW COUNT VALIDATION (DATA FLOW CHECK)
-- =========================================================

-- RAW vs STAGING
SELECT 'RAW_TRANSACTIONS' AS table_name, COUNT(*) FROM RAW.RAW_TRANSACTIONS
UNION ALL
SELECT 'STG_TRANSACTIONS', COUNT(*) FROM STAGING.STG_TRANSACTIONS;

-- STAGING vs FACT
SELECT 'STG_TRANSACTIONS' AS table_name, COUNT(*) FROM STAGING.STG_TRANSACTIONS
UNION ALL
SELECT 'FACT_TRANSACTIONS', COUNT(*) FROM CORE.FACT_TRANSACTIONS;


-- =========================================================
-- 3. NULL / DATA QUALITY CHECKS
-- =========================================================

-- Check critical NULLs in staging
SELECT *
FROM STAGING.STG_TRANSACTIONS
WHERE transaction_time IS NULL
   OR amount IS NULL
   OR fraud_flag IS NULL;

-- Check invalid amounts
SELECT *
FROM STAGING.STG_TRANSACTIONS
WHERE amount < 0;

-- Check missing customer mapping in FACT
SELECT *
FROM CORE.FACT_TRANSACTIONS
WHERE customer_sk IS NULL;


-- =========================================================
-- 4. DUPLICATE CHECKS
-- =========================================================

-- Duplicate transactions in FACT
SELECT transaction_id, COUNT(*)
FROM CORE.FACT_TRANSACTIONS
GROUP BY transaction_id
HAVING COUNT(*) > 1;

-- Duplicate alerts
SELECT transaction_id, COUNT(*)
FROM CORE.FRAUD_ALERTS
GROUP BY transaction_id
HAVING COUNT(*) > 1;


-- =========================================================
-- 5. SCD TYPE 2 VALIDATION
-- =========================================================

-- Ensure ONLY ONE current record per customer
SELECT customer_id, COUNT(*)
FROM CORE.DIM_CUSTOMERS
WHERE is_current = TRUE
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Check history exists (customers with >1 records)
SELECT customer_id, COUNT(*) AS versions
FROM CORE.DIM_CUSTOMERS
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- View history for a sample customer
SELECT *
FROM CORE.DIM_CUSTOMERS
WHERE customer_id = 1
ORDER BY effective_date;


-- =========================================================
-- 6. FRAUD LOGIC VALIDATION
-- =========================================================

-- Confirm fraud_flag alerts exist
SELECT *
FROM CORE.FRAUD_ALERTS
WHERE alert_reason = 'Confirmed Fraud'
LIMIT 10;

-- High amount alerts
SELECT *
FROM CORE.FRAUD_ALERTS
WHERE alert_reason = 'High Amount Transaction'
LIMIT 10;

-- Velocity alerts
SELECT *
FROM CORE.FRAUD_ALERTS
WHERE alert_reason = 'High Velocity Transactions'
LIMIT 10;


-- =========================================================
-- 7. PIPELINE VALIDATION (STREAM + TASK)
-- =========================================================

-- Check if stream has pending data
SELECT * FROM STAGING.STREAM_TRANSACTIONS;

SELECT * FROM CORE.STREAM_FACT_TRANSACTIONS;

-- Check task execution history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
ORDER BY SCHEDULED_TIME DESC
LIMIT 10;


-- =========================================================
-- 8. END-TO-END TEST (SIMULATION)
-- =========================================================

-- Step 1: Insert test transaction
INSERT INTO STAGING.STG_TRANSACTIONS (
    transaction_id,
    customer_id,
    transaction_time,
    amount,
    fraud_flag,
    created_at,
    updated_at
)
VALUES (
    UUID_STRING(),
    1,
    CURRENT_TIMESTAMP(),
    9999,   -- High amount trigger
    0,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
);

-- Step 2: Wait for task execution, then validate

-- Check FACT
SELECT *
FROM CORE.FACT_TRANSACTIONS
ORDER BY transaction_time DESC
LIMIT 5;

-- Check ALERTS
SELECT *
FROM CORE.FRAUD_ALERTS
ORDER BY alert_timestamp DESC
LIMIT 5;