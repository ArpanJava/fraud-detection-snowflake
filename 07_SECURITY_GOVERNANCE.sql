/* =========================================================
   STEP 1: CREATE ROLES
   ========================================================= */

CREATE OR REPLACE ROLE FRAUD_ADMIN;
CREATE OR REPLACE ROLE FRAUD_ANALYST;
CREATE OR REPLACE ROLE FRAUD_VIEWER;


/* =========================================================
   STEP 2: WAREHOUSE ACCESS
   ========================================================= */

GRANT USAGE ON WAREHOUSE FRAUD_ETL_WH TO ROLE FRAUD_ADMIN;

GRANT USAGE ON WAREHOUSE FRAUD_ANALYTICS_WH TO ROLE FRAUD_ANALYST;
GRANT USAGE ON WAREHOUSE FRAUD_ANALYTICS_WH TO ROLE FRAUD_VIEWER;


/* =========================================================
   STEP 3: DATABASE & SCHEMA ACCESS
   ========================================================= */

GRANT USAGE ON DATABASE FRAUD_DETECTION_DB TO ROLE FRAUD_ADMIN;
GRANT USAGE ON DATABASE FRAUD_DETECTION_DB TO ROLE FRAUD_ANALYST;
GRANT USAGE ON DATABASE FRAUD_DETECTION_DB TO ROLE FRAUD_VIEWER;

GRANT USAGE ON ALL SCHEMAS IN DATABASE FRAUD_DETECTION_DB TO ROLE FRAUD_ADMIN;
GRANT USAGE ON ALL SCHEMAS IN DATABASE FRAUD_DETECTION_DB TO ROLE FRAUD_ANALYST;
GRANT USAGE ON ALL SCHEMAS IN DATABASE FRAUD_DETECTION_DB TO ROLE FRAUD_VIEWER;


/* =========================================================
   STEP 4: TABLE-LEVEL PERMISSIONS
   ========================================================= */

-- Admin → Full control
GRANT ALL PRIVILEGES ON ALL TABLES IN DATABASE FRAUD_DETECTION_DB 
TO ROLE FRAUD_ADMIN;

-- Analyst → Read-only
GRANT SELECT ON ALL TABLES IN DATABASE FRAUD_DETECTION_DB 
TO ROLE FRAUD_ANALYST;

-- Viewer → Restricted read
GRANT SELECT ON ALL TABLES IN DATABASE FRAUD_DETECTION_DB 
TO ROLE FRAUD_VIEWER;


/* =========================================================
   STEP 5: MASKING POLICIES
   ========================================================= */

-- EMAIL MASK
CREATE OR REPLACE MASKING POLICY EMAIL_MASK 
AS (val STRING)
RETURNS STRING ->
CASE
    WHEN CURRENT_ROLE() = 'FRAUD_ADMIN' THEN val
    WHEN CURRENT_ROLE() = 'FRAUD_ANALYST' 
        THEN REGEXP_REPLACE(val, '(^.).*(@.*$)', '\\1****\\2')
    ELSE 'MASKED'
END;


-- PHONE MASK
CREATE OR REPLACE MASKING POLICY PHONE_MASK 
AS (val STRING)
RETURNS STRING ->
CASE
    WHEN CURRENT_ROLE() = 'FRAUD_ADMIN' THEN val
    WHEN CURRENT_ROLE() = 'FRAUD_ANALYST' 
        THEN CONCAT('XXXXXX', RIGHT(val, 4))
    ELSE 'MASKED'
END;


-- ADDRESS MASK
CREATE OR REPLACE MASKING POLICY ADDRESS_MASK 
AS (val STRING)
RETURNS STRING ->
CASE
    WHEN CURRENT_ROLE() = 'FRAUD_ADMIN' THEN val
    WHEN CURRENT_ROLE() = 'FRAUD_ANALYST' THEN 'PARTIAL_ADDRESS'
    ELSE 'MASKED'
END;


/* =========================================================
   STEP 6: APPLY MASKING POLICIES
   ========================================================= */

ALTER TABLE CORE.DIM_CUSTOMERS 
MODIFY COLUMN email SET MASKING POLICY EMAIL_MASK;

ALTER TABLE CORE.DIM_CUSTOMERS 
MODIFY COLUMN phone_number SET MASKING POLICY PHONE_MASK;

ALTER TABLE CORE.DIM_CUSTOMERS 
MODIFY COLUMN address SET MASKING POLICY ADDRESS_MASK;


/* =========================================================
   STEP 7: SECURE VIEW FOR ANALYTICS
   ========================================================= */

CREATE OR REPLACE SECURE VIEW ANALYTICS.VW_CUSTOMERS_SECURE AS
SELECT 
    customer_id,
    customer_name,
    email,
    phone_number,
    address,
    effective_date,
    end_date,
    is_current
FROM CORE.DIM_CUSTOMERS;


GRANT SELECT ON ANALYTICS.VW_CUSTOMERS_SECURE TO ROLE FRAUD_ANALYST;
GRANT SELECT ON ANALYTICS.VW_CUSTOMERS_SECURE TO ROLE FRAUD_VIEWER;


/* =========================================================
   STEP 8: TESTING ACCESS CONTROL
   ========================================================= */

-- ADMIN (Full visibility)
USE ROLE FRAUD_ADMIN;
SELECT * FROM CORE.DIM_CUSTOMERS WHERE customer_id = 1;

-- ANALYST (Masked data)
USE ROLE FRAUD_ANALYST;
SELECT * FROM CORE.DIM_CUSTOMERS WHERE customer_id = 1;

-- VIEWER (Fully masked)
USE ROLE FRAUD_VIEWER;
SELECT * FROM CORE.DIM_CUSTOMERS WHERE customer_id = 1;