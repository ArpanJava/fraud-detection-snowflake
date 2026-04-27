-- =========================================================
-- 1. SET CONTEXT
-- =========================================================

USE WAREHOUSE FRAUD_ETL_WH;
USE DATABASE FRAUD_DETECTION_DB;
USE SCHEMA CORE;


-- =========================================================
-- 2. CREATE CORE TABLES
-- =========================================================

-- Dimension Table (SCD Type 2)
CREATE OR REPLACE TABLE DIM_CUSTOMERS (
    customer_sk INTEGER AUTOINCREMENT,
    customer_id INTEGER NOT NULL,
    customer_name STRING,
    email STRING,
    phone_number STRING,
    address STRING,
    effective_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    end_date TIMESTAMP DEFAULT NULL,
    is_current BOOLEAN DEFAULT TRUE
);

-- Fact Table
CREATE OR REPLACE TABLE FACT_TRANSACTIONS (
    transaction_id STRING PRIMARY KEY,
    customer_sk INTEGER NOT NULL,
    transaction_time TIMESTAMP,
    amount FLOAT,
    fraud_flag INTEGER,
    risk_score FLOAT
);


-- =========================================================
-- 3. SCD TYPE 2 LOGIC (STAGING → DIM_CUSTOMERS)
-- =========================================================

-- STEP 1: Expire old records when data changes
UPDATE CORE.DIM_CUSTOMERS target
SET 
    end_date = CURRENT_TIMESTAMP(),
    is_current = FALSE
FROM STAGING.STG_CUSTOMERS source
WHERE target.customer_id = source.customer_id
  AND target.is_current = TRUE
  AND (
        NVL(target.customer_name,'') != NVL(source.customer_name,'') OR
        NVL(target.email,'')         != NVL(source.email,'') OR
        NVL(target.phone_number,'')  != NVL(source.phone_number,'') OR
        NVL(target.address,'')       != NVL(source.address,'')
      );


-- STEP 2: Insert new records (new or changed customers)
INSERT INTO CORE.DIM_CUSTOMERS (
    customer_id,
    customer_name,
    email,
    phone_number,
    address,
    effective_date,
    end_date,
    is_current
)
SELECT
    source.customer_id,
    source.customer_name,
    source.email,
    source.phone_number,
    source.address,
    CURRENT_TIMESTAMP(),
    NULL,
    TRUE
FROM STAGING.STG_CUSTOMERS source
WHERE NOT EXISTS (
    SELECT 1
    FROM CORE.DIM_CUSTOMERS t
    WHERE t.customer_id = source.customer_id
      AND t.is_current = TRUE
      AND NVL(t.customer_name,'') = NVL(source.customer_name,'')
      AND NVL(t.email,'')         = NVL(source.email,'')
      AND NVL(t.phone_number,'')  = NVL(source.phone_number,'')
      AND NVL(t.address,'')       = NVL(source.address,'')
);


-- =========================================================
-- 4. DATA VALIDATION (SCD CHECKS)
-- =========================================================

-- Ensure only ONE active record per customer
SELECT customer_id, COUNT(*)
FROM CORE.DIM_CUSTOMERS
WHERE is_current = TRUE
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- View history of a customer
SELECT *
FROM CORE.DIM_CUSTOMERS
ORDER BY customer_id, effective_date;

-- Check current records
SELECT *
FROM CORE.DIM_CUSTOMERS
WHERE is_current = TRUE
LIMIT 10;