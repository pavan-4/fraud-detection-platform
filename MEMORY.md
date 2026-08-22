# PROJECT MEMORY — Fraud Detection Platform
> Paste this file at the start of every new Claude session to restore full context instantly.
> Keep it updated after each layer is completed. Do NOT add long explanations — keep it short.

---

## 🧠 Who I Am
- Name: Pavan
- Goal: Build a production-level Real-Time Payment Fraud Detection Platform
- Purpose: Portfolio project targeting Data Engineer roles in Ireland (JPMorgan, Paymentology, Citco, Bloomberg)
- Learning style: Layer by layer, hands-on, explain WHY as we go
- GitHub repo: https://github.com/pavan-4/fraud-detection-platform
- Local folder: C:\Users\Pavan\Desktop\fraud_detection_DE

---

## 🎯 Project Goal
Build an end-to-end real-time fraud detection data pipeline that:
- Ingests fake card transaction events via Azure Event Hubs (Kafka-compatible)
- Processes them with PySpark Structured Streaming on Databricks
- Stores data in Delta Lake (Bronze / Silver / Gold medallion) via Unity Catalog
- Transforms with dbt models (staging → intermediate → fraud mart → PSD2 report)
- Orchestrates with Apache Airflow
- Deploys infra with Terraform + GitHub Actions CI/CD

---

## 🛠️ Fixed Tech Stack
| Layer         | Tool                          | Where it runs              |
|--------------|-------------------------------|----------------------------|
| Streaming     | Azure Event Hubs (Kafka API)  | Azure (Standard tier)      |
| Processing    | PySpark Structured Streaming  | Databricks Free Edition    |
| Storage       | Delta Lake + Unity Catalog    | Databricks (serverless)    |
| Transform     | dbt Core (open source)        | Databricks                 |
| Orchestration | Apache Airflow                | Local Docker               |
| CI/CD         | GitHub Actions                | GitHub (free)              |
| IaC           | Terraform                     | Local                      |
| Language      | Python 3.10+                  | Everywhere                 |

---

## 📁 Project Structure
```
fraud-detection-platform/
├── MEMORY.md                        ← This file (gitignored — NEVER commit)
├── DECISIONS.md
├── README.md
├── Dockerfile                       ← Custom Airflow image (pre-installs providers + dbt)
├── docker-compose.yml               ← Kafka + Airflow + Postgres
├── producer/
│   └── transaction_producer.py      ← confluent-kafka, sends to Azure Event Hubs
├── databricks/                      ← Notebooks committed to GitHub as .ipynb
│   ├── bronze_ingestion.ipynb
│   ├── silver_enrichment.ipynb
│   └── gold_fraud_alerts.ipynb
├── dbt/
│   └── fraud_platform/
│       ├── profiles.yml             ← dev + ci targets (ci reads DATABRICKS_TOKEN env var)
│       ├── models/staging/stg_transactions.sql
│       ├── models/intermediate/int_fraud_scored.sql
│       ├── models/marts/mart_fraud_alerts.sql
│       └── models/staging/schema.yml  ← 11 data quality tests
├── airflow/
│   └── dags/
│       └── fraud_platform_pipeline.py
├── terraform/                       ← Terraform IaC (NEXT)
└── .github/workflows/
    └── ci_dbt_tests.yml             ← GitHub Actions CI (COMPLETE)
```

---

## 🔒 Fixed Decisions

### Azure Event Hubs
- Namespace: fraud-platform-eh.servicebus.windows.net:9093
- Topics: raw.transactions (3 partitions), raw.transactions.dead-letter (1 partition)
- Tier: Standard (Basic does NOT support Kafka protocol)
- Auth: SASL_SSL + PLAIN, username="$ConnectionString"

### Databricks
- Workspace URL: https://dbc-228421d6-53f1.cloud.databricks.com
- Compute: Serverless only (Free Edition does NOT support classic clusters)
- Catalog: workspace (not main)
- Secrets scope: fraud-platform, key: event-hubs-connection-string
- Access: dbutils.secrets.get("fraud-platform", "event-hubs-connection-string")
- CLI config: C:\Users\Pavan\.databrickscfg
- Notebook paths in workspace: /Users/pavankumar.ga14@gmail.com/bronze_ingestion etc.

### Unity Catalog Storage Paths
- Volume:      workspace.fraud_platform.data
- Bronze:      /Volumes/workspace/fraud_platform/data/bronze/transactions
- Silver:      /Volumes/workspace/fraud_platform/data/silver/transactions
- Gold:        /Volumes/workspace/fraud_platform/data/gold/
- Checkpoint:  /Volumes/workspace/fraud_platform/data/checkpoints/bronze

### Kafka / Streaming
- Topic: raw.transactions
- Producer key: card_id (ordering per card within partition)
- Rate: 2 txns/sec, 5% fraud rate
- Fraud types: high_amount, foreign_country, rapid_succession, suspicious_mcc

### Transaction Schema Fields
transaction_id, card_id, customer_id, merchant_id, merchant_name,
merchant_category_code, amount_local, currency_code, amount_eur,
country_code, terminal_type, event_timestamp, ip_address, latitude, longitude,
_fraud_simulation_type

### Airflow
- URL: http://localhost:8088 (admin / admin)
- Databricks connection: Admin → Connections → databricks_default
  - Connection Type: Databricks
  - Host: https://dbc-228421d6-53f1.cloud.databricks.com
  - Password: <databricks token>  (NOT in Extra — use Password field)
- Start command: docker-compose up -d postgres airflow-init airflow-webserver airflow-scheduler
- DATABRICKS_TOKEN must be set: $env:DATABRICKS_TOKEN="token"

### GitHub Actions CI
- File: .github/workflows/ci_dbt_tests.yml
- Triggers: push or PR to main when dbt/** files change
- Runs: dbt test --profiles-dir . --target ci
- Secret needed in GitHub: DATABRICKS_TOKEN
- profiles.yml ci target: host WITHOUT https:// prefix (critical gotcha)
- CI runs in ~50 seconds, all 11 dbt tests pass ✅

---

## ✅ Progress Tracker
- [✅] Layer 1 — Kafka producer + Azure Event Hubs
- [✅] Layer 2 — Bronze / Silver / Gold medallion on Databricks
- [✅] Layer 3 — dbt models (stg → int → mart, 11 tests passing)
- [✅] Layer 4 — Airflow DAG (all 5 tasks green, end-to-end pipeline running)
- [✅] Layer 5 — GitHub Actions CI/CD (dbt tests auto-run on every push)
- [ ] Layer 6 — Terraform (IaC for Azure Event Hubs)  ← NEXT
- [ ] Layer 7 — README polish + CI badge (portfolio finish)

**Current layer: Layer 6 — Terraform IaC**

---

## 📝 Layer 3 — What Was Built (COMPLETE)

### dbt Models (dbt/fraud_platform/)
- stg_transactions.sql — cleans Bronze table, casts types, filters nulls
- int_fraud_scored.sql — computes all 4 fraud signals + risk_score (0-100) + risk_label
- mart_fraud_alerts.sql — materialized as TABLE, HIGH-risk txns only + alert_reason string
- schema.yml — 11 data quality tests (unique, not_null, accepted_values)
- All 3 models run: PASS=3, WARN=0, ERROR=0
- All 11 tests pass: PASS=11, WARN=0, ERROR=0, SKIP=0

### Key gotchas
- Unity Catalog table names with dots need backticks: `bronze_transactions`
- RANGE BETWEEN syntax: use PRECEDING not CLOSED
- Tables must be registered via saveAsTable() before dbt can see them
- dbt_project.yml warned about unused example config — harmless

---

## 📝 Layer 4 — What Was Built (COMPLETE)

### Airflow Setup
- Custom Dockerfile extends apache/airflow:2.9.0, pre-installs providers + dbt
- docker-compose.yml: postgres + airflow-init + airflow-webserver + airflow-scheduler
- DAG: fraud_platform_pipeline, schedule */5 * * * * (every 5 min)
- 5 tasks: bronze_ingestion → silver_enrichment → gold_fraud_alerts → dbt_run → dbt_test

### Key gotchas
- Use Dockerfile + build: . instead of _PIP_ADDITIONAL_REQUIREMENTS (dbt-databricks conflicts)
- Databricks Free Edition: serverless only — DatabricksSubmitRunOperator needs multi-task format:
  json={"run_name": "...", "tasks": [{"task_key": "...", "notebook_task": {...}}], "queue": {"enabled": True}}
- Token must go in Password field of Airflow connection, NOT in Extra JSON
- Notebook paths in Airflow: /Users/pavankumar.ga14@gmail.com/bronze_ingestion (no subfolder)
- First run: webserver takes 3-5 min to build image

---

## 📝 Layer 5 — What Was Built (COMPLETE)

### GitHub Actions CI (.github/workflows/ci_dbt_tests.yml)
- Triggers on push/PR to main when any dbt/** file changes
- Installs dbt-databricks==1.7.14 on ubuntu-latest
- Runs: cd dbt/fraud_platform && dbt test --profiles-dir . --target ci
- DATABRICKS_TOKEN injected from GitHub Secrets → profiles.yml ci target
- All 11 dbt tests pass in ~50 seconds ✅

### profiles.yml (dbt/fraud_platform/profiles.yml)
- Two targets: dev (local, reads DBT_TOKEN env var) and ci (GitHub Actions)
- CRITICAL: host must NOT include https:// — use bare hostname only
  ✅ host: dbc-228421d6-53f1.cloud.databricks.com
  ❌ host: https://dbc-228421d6-53f1.cloud.databricks.com  ← breaks DNS

### Key gotchas
- Remove https:// from host in profiles.yml — dbt-databricks parses it as the hostname itself
- GitHub Secret name must exactly match: DATABRICKS_TOKEN

---

## ⚠️ Known Issues / Gotchas
- kafka-python-ng SASL incompatible with Event Hubs → use confluent-kafka
- Databricks serverless: use kafkashaded.org.apache.kafka prefix in SASL_JAAS
- Databricks serverless: ProcessingTime trigger not supported → use AvailableNow
- DBFS root (/FileStore/) disabled → use Unity Catalog Volumes
- Databricks catalog is workspace (not main)
- Unity Catalog registered table is a STATIC snapshot — live row counts use Volume path
- MEMORY.md gitignored — never commit
- .env gitignored — never commit (contains EVENT_HUBS_CONNECTION_STRING)

---

## 💬 How to Start a New Session
```
I'm building a fraud detection pipeline with Claude.
Here is my MEMORY.md — please read it and continue from exactly where we left off.

[paste the contents of this file]

Today I want to: [describe what you want to do]
```

---
_Last updated: 2026-08-22_
