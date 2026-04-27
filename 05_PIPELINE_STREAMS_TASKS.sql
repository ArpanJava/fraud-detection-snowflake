-- =========================================================
-- 1. SET CONTEXT
-- =========================================================

USE WAREHOUSE FRAUD_ETL_WH;
USE DATABASE FRAUD_DETECTION_DB;

CREATE SCHEMA IF NOT EXISTS PIPELINE;


-- =========================================================
-- 2. CREATE STREAM ON STAGING (CHANGE DATA CAPTURE)
-- =========================================================

CREATE OR REPLACE STREAM STAGING.STREAM_TRANSACTIONS
ON TABLE STAGING.STG_TRANSACTIONS
APPEND_ONLY = TRUE;


-- =========================================================
-- 3. CREATE TASK: STAGING → FACT_TRANSACTIONS
-- =========================================================

CREATE OR REPLACE TASK PIPELINE.TRANSACTIONS_TASK
WAREHOUSE = FRAUD_ETL_WH
WHEN SYSTEM$STREAM_HAS_DATA('STAGING.STREAM_TRANSACTIONS')
AS

INSERT INTO CORE.FACT_TRANSACTIONS
WITH STREAM_DATA AS (
    SELECT *
    FROM STAGING.STREAM_TRANSACTIONS
    WHERE METADATA$ACTION = 'INSERT'
)
SELECT 
    s.transaction_id,
    d.customer_sk,
    s.transaction_time,
    s.amount,
    s.fraud_flag,
    NULL AS risk_score
FROM STREAM_DATA s
JOIN CORE.DIM_CUSTOMERS d
    ON s.customer_id = d.customer_id
    AND d.is_current = TRUE
WHERE d.customer_sk IS NOT NULL
AND NOT EXISTS (
    SELECT 1
    FROM CORE.FACT_TRANSACTIONS f
    WHERE f.transaction_id = s.transaction_id
);


-- =========================================================
-- 4. CREATE STREAM ON FACT TABLE
-- =========================================================

CREATE OR REPLACE STREAM CORE.STREAM_FACT_TRANSACTIONS
ON TABLE CORE.FACT_TRANSACTIONS
APPEND_ONLY = TRUE;


-- =========================================================
-- 5. CREATE TASK: FRAUD ALERT GENERATION
-- =========================================================

CREATE OR REPLACE TASK PIPELINE.FRAUD_ALERT_TASK
AFTER PIPELINE.TRANSACTIONS_TASK
AS

INSERT INTO CORE.FRAUD_ALERTS (
    alert_id,
    transaction_id,
    customer_sk,
    alert_type,
    alert_reason,
    alert_timestamp
)

WITH NEW_TXNS AS (
    SELECT *
    FROM CORE.STREAM_FACT_TRANSACTIONS
    WHERE METADATA$ACTION = 'INSERT'
),

VELOCITY_CHECK AS (
    SELECT 
        customer_sk,
        transaction_id,
        COUNT(*) OVER (
            PARTITION BY customer_sk
            ORDER BY transaction_time
            RANGE BETWEEN INTERVAL '10 MINUTES' PRECEDING AND CURRENT ROW
        ) AS txn_count_10min
    FROM CORE.FACT_TRANSACTIONS
)

SELECT 
    UUID_STRING(),
    n.transaction_id,
    n.customer_sk,
    'Real-Time',

    CASE 
        WHEN n.fraud_flag = 1 THEN 'Confirmed Fraud'
        WHEN n.amount > 500 THEN 'High Amount Transaction'
        WHEN v.txn_count_10min > 5 THEN 'High Velocity Transactions'
        ELSE 'Unknown'
    END,

    CURRENT_TIMESTAMP()

FROM NEW_TXNS n
LEFT JOIN VELOCITY_CHECK v
    ON n.transaction_id = v.transaction_id

WHERE 
(
    n.fraud_flag = 1
    OR n.amount > 500
    OR v.txn_count_10min > 5
)
AND NOT EXISTS (
    SELECT 1
    FROM CORE.FRAUD_ALERTS fa
    WHERE fa.transaction_id = n.transaction_id
);


-- =========================================================
-- 6. ENABLE TASKS
-- =========================================================

ALTER TASK PIPELINE.TRANSACTIONS_TASK RESUME;
ALTER TASK PIPELINE.FRAUD_ALERT_TASK RESUME;


-- =========================================================
-- 7. VALIDATION / TESTING
-- =========================================================

-- Check if stream has data
SELECT * FROM STAGING.STREAM_TRANSACTIONS;

-- Check fact table population
SELECT COUNT(*) FROM CORE.FACT_TRANSACTIONS;

-- Check fraud alerts
SELECT * FROM CORE.FRAUD_ALERTS LIMIT 10;

-- Check suspicious transactions
SELECT *
FROM CORE.FRAUD_ALERTS
WHERE alert_reason IS NOT NULL;