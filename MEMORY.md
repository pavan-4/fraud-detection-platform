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
- Ingests fake card transaction events via Kafka
- Processes them with PySpark Structured Streaming
- Stores data in Delta Lake (Bronze / Silver / Gold medallion)
- Transforms with dbt models (staging → intermediate → fraud mart → PSD2 report)
- Orchestrates with Apache Airflow
- Deploys infra with Terraform + GitHub Actions CI/CD

---

## 🛠️ Fixed Tech Stack (DO NOT suggest alternatives)
| Layer        | Tool                        | Where it runs         |
|-------------|-----------------------------|-----------------------|
| Streaming    | Apache Kafka                | Local Docker          |
| Processing   | PySpark Structured Streaming| Databricks Community Edition |
| Storage      | Delta Lake                  | Databricks CE (DBFS)  |
| Transform    | dbt Core (open source)      | Databricks CE         |
| Orchestration| Apache Airflow              | Local Docker          |
| CI/CD        | GitHub Actions              | GitHub (free)         |
| IaC          | Terraform                   | Local                 |
| Language     | Python 3.10+                | Everywhere            |
| Cost         | €0 — fully free             |                       |

---

## 📁 Project Structure (Fixed — do not change)
```
fraud-detection-platform/
├── MEMORY.md                  ← This file
├── DECISIONS.md               ← Key architectural decisions
├── README.md                  ← Progress tracker + setup guide
├── docker-compose.yml         ← Kafka + Zookeeper + Schema Registry + Kafka UI + Airflow
├── producer/
│   ├── transaction_producer.py   ← Fake card transaction event generator
│   └── schemas/
│       └── transaction.avsc      ← Avro schema for transaction events
├── spark_jobs/
│   ├── bronze_ingestion.py       ← Kafka → Delta Lake Bronze
│   ├── silver_enrichment.py      ← Bronze → Silver (features + enrichment)
│   └── utils/
│       └── spark_session.py      ← Shared SparkSession factory
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_transactions.sql
│   │   │   ├── stg_customers.sql
│   │   │   └── schema.yml
│   │   ├── intermediate/
│   │   │   ├── int_transaction_enriched.sql
│   │   │   └── int_fraud_scored.sql
│   │   └── marts/
│   │       ├── mart_fraud_daily_summary.sql
│   │       └── mart_regulatory_psd2.sql
│   └── tests/
│       └── custom_fraud_rate_anomaly.sql
├── airflow/
│   └── dags/
│       └── fraud_platform_pipeline.py
├── terraform/
│   ├── main.tf
│   ├── kafka_cluster.tf
│   ├── variables.tf
│   └── outputs.tf
└── .github/
    └── workflows/
        ├── ci_dbt_tests.yml
        └── cd_deploy_infra.yml
```

---

## 🔒 Fixed Decisions (DO NOT change these — see DECISIONS.md for reasons)
- Kafka topic name: `raw.transactions`
- Dead-letter topic: `raw.transactions.dead_letter`
- Delta Lake paths:
  - Bronze: `/FileStore/fraud-platform/bronze/transactions`
  - Silver: `/FileStore/fraud-platform/silver/transactions`
  - Gold:   `/FileStore/fraud-platform/gold/`
  - Checkpoints: `/FileStore/fraud-platform/checkpoints/`
- Transaction schema fields: transaction_id, card_id, customer_id, merchant_id, merchant_name, merchant_category_code, amount_local, currency_code, amount_eur, country_code, terminal_type, event_timestamp, ip_address, latitude, longitude
- Avro schema namespace: `com.fraudplatform.payments`
- Kafka bootstrap: `localhost:9092` (local Docker)
- Databricks CE cluster: single node, DBR 14.3 LTS

---

## ✅ Progress Tracker
- [✅] Layer 1 — Kafka local environment + transaction producer
- [ ] Layer 2 — PySpark Bronze job (Kafka → Delta Lake)
- [ ] Layer 3 — PySpark Silver job (enrichment + rolling features)
- [ ] Layer 4 — dbt models (staging → intermediate → marts)
- [ ] Layer 5 — Airflow DAG (orchestration)
- [ ] Layer 6 — GitHub Actions CI/CD + portfolio polish

**Current layer: PySpark Bronze job (Kafka → Delta Lake)**
**Last completed: Layer 1 — Kafka + Transaction Producer ✅**

---

## 📝 Session Log (update after every session)
| Date | Layer | What we did | Where we stopped |
|------|-------|-------------|-----------------|
| 2026-08-19 | Layer 1 | Fixed kafka-init bug (multiline command), fixed Python 3.12 kafka-python-ng issue, producer sending live transactions | Ready for Layer 2 — PySpark Bronze on Databricks |

---

## ⚠️ Known Issues / Blockers
_None yet — add here as they come up_
- Azure Event Hubs Basic tier does NOT support Kafka protocol — must use Standard tier
- kafka-python-ng has SASL incompatibility with Event Hubs — use confluent-kafka instead
---

## 💬 How to Start a New Session
Paste this exact message at the top of a new Claude conversation:

```
I'm building a fraud detection pipeline with Claude.
Here is my MEMORY.md — please read it and continue from exactly where we left off.
Do not suggest different tools, structure, or approaches than what's documented here.

[paste the contents of this file]

Today I want to: [describe what you want to do]
```

---
_Last updated: 2026-08-19_
