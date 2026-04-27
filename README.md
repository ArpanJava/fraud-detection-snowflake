#  Fraud Detection Data Pipeline (Snowflake)

##  Overview
This project demonstrates an end-to-end fraud detection data pipeline built using Snowflake.

It simulates real-world financial transaction processing with near real-time fraud detection and data governance.

---

##  Architecture

RAW → STAGING → CORE → PIPELINE → ANALYTICS

---

##  Tech Stack

- Snowflake (SQL)
- Streams & Tasks (CDC)
- SCD Type 2 Modeling
- RBAC + Masking Policies

---

##  Data Pipeline

1. RAW Layer → Ingest CSV into VARIANT
2. STAGING Layer → Transform & clean data
3. CORE Layer → Fact + Dimension tables
4. PIPELINE → Streams + Tasks for real-time load
5. FRAUD DETECTION → Rule-based alerts

---

##  Fraud Logic

- High transaction amount (>500)
- High transaction velocity (within 10 minutes)
- Known fraud flag

---

##  Data Governance

- Role-based access control (RBAC)
- Dynamic masking policies
- Secure views

---

##  Key Features

- Real-time data processing
- SCD Type 2 implementation
- Idempotent pipeline design
- Data validation checks


