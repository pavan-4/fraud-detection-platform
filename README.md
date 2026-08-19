# 🏦 Real-Time Payment Fraud Detection Platform

> A production-level data engineering portfolio project targeting Data Engineer roles in Ireland.
> Built with: Kafka · PySpark · Delta Lake · dbt · Airflow · Terraform · GitHub Actions

---

## 🎯 What This Project Does

Simulates a real-time payment fraud detection pipeline similar to what JPMorgan Chase,
Paymentology, and Citco run in production in Dublin.

- **Ingests** fake card transaction events via Apache Kafka (10,000+ events/minute)
- **Processes** them in real-time with PySpark Structured Streaming
- **Stores** data in a Delta Lake medallion architecture (Bronze → Silver → Gold)
- **Transforms** with dbt models including a PSD2 EU regulatory fraud report
- **Orchestrates** everything with Apache Airflow
- **Deploys** infrastructure with Terraform + GitHub Actions CI/CD

---

## 🛠️ Tech Stack

| Layer | Tool | Cost |
|-------|------|------|
| Event streaming | Apache Kafka (Docker) | Free |
| Stream processing | PySpark on Databricks CE | Free |
| Storage | Delta Lake on Databricks CE | Free |
| Transformation | dbt Core | Free |
| Orchestration | Apache Airflow (Docker) | Free |
| CI/CD | GitHub Actions | Free |
| IaC | Terraform | Free |
| **Total** | | **€0** |

---

## 📋 Build Progress

- [ ] **Layer 1** — Kafka local environment + transaction event producer
- [ ] **Layer 2** — PySpark Bronze job (Kafka → Delta Lake)
- [ ] **Layer 3** — PySpark Silver job (enrichment + rolling window features)
- [ ] **Layer 4** — dbt models (staging → intermediate → fraud mart + PSD2 report)
- [ ] **Layer 5** — Airflow DAG (full pipeline orchestration)
- [ ] **Layer 6** — GitHub Actions CI/CD + Terraform + portfolio polish

---

## 🗂️ Project Structure

```
fraud-detection-platform/
├── MEMORY.md                        ← Claude session context file
├── DECISIONS.md                     ← All architectural decisions + reasons
├── README.md                        ← This file
├── docker-compose.yml               ← Kafka + Zookeeper + Schema Registry + Airflow
│
├── producer/                        ← LAYER 1
│   ├── transaction_producer.py      ← Fake card transaction generator
│   └── schemas/
│       └── transaction.avsc         ← Avro event schema
│
├── spark_jobs/                      ← LAYERS 2 & 3
│   ├── bronze_ingestion.py          ← Kafka → Delta Lake Bronze
│   ├── silver_enrichment.py         ← Bronze → Silver (features + enrichment)
│   └── utils/
│       └── spark_session.py         ← Shared SparkSession factory
│
├── dbt/                             ← LAYER 4
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/
│       ├── intermediate/
│       └── marts/
│
├── airflow/                         ← LAYER 5
│   └── dags/
│       └── fraud_platform_pipeline.py
│
├── terraform/                       ← LAYER 6
│   ├── main.tf
│   └── variables.tf
│
└── .github/
    └── workflows/                   ← LAYER 6
        ├── ci_dbt_tests.yml
        └── cd_deploy_infra.yml
```

---

## 🚀 Quick Start (after full build)

```bash
# 1. Start Kafka locally
docker-compose up -d

# 2. Run the transaction producer
python producer/transaction_producer.py

# 3. Upload spark_jobs/ to Databricks CE and run notebooks

# 4. Run dbt models
cd dbt && dbt run && dbt test

# 5. Start Airflow
docker-compose up airflow
```

---

## 📚 Why Each Tool Was Chosen

See [DECISIONS.md](./DECISIONS.md) for the full reasoning behind every architectural choice.

---

## 👤 Built By

Pavan Kumar — Data Engineer Portfolio Project
Targeting: JPMorgan Chase · Paymentology · Citco · Bloomberg · Optum (Dublin, Ireland)
