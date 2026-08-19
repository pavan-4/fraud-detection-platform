# DECISIONS.md — Architectural Decisions Log
> Every key decision in this project is recorded here with the reason.
> When starting a new Claude session, paste this alongside MEMORY.md.

---

## Infrastructure Decisions

| Decision | Choice | Reason |
|----------|--------|--------|
| Kafka hosting | Local Docker | Free, no cloud cost, production-equivalent behaviour |
| Spark hosting | Databricks Community Edition | Free tier, native Delta Lake support |
| Storage | Delta Lake on DBFS (Databricks CE) | Free, ACID transactions, time travel built-in |
| Orchestration | Airflow via Docker | Free, industry standard, local dev friendly |
| CI/CD | GitHub Actions | Free tier, integrates with GitHub repo |
| IaC | Terraform (local plan only) | Portfolio demonstration — no cloud apply needed |
| Cost | €0 total | Everything runs free locally or on free tiers |

---

## Kafka Decisions

| Decision | Choice | Reason |
|----------|--------|--------|
| Kafka version | 7.5.x (Confluent) | Stable, Schema Registry compatible |
| Topic name | `raw.transactions` | Dot-notation = production naming convention |
| Dead-letter topic | `raw.transactions.dead_letter` | Separate bad records from good, never mix |
| Partitions | 3 (local dev) | Enough for parallelism demo without resource overload |
| Replication | 1 (local dev) | Single broker locally — would be 3 in production |
| Message format | JSON (dev) → Avro (prod) | JSON easier to inspect locally; Avro in prod for schema enforcement |
| Retention | 48 hours | Enough to replay events if pipeline fails |

---

## Delta Lake Decisions

| Decision | Choice | Reason |
|----------|--------|--------|
| Bronze path | `/FileStore/fraud-platform/bronze/transactions` | DBFS path accessible from CE notebooks |
| Silver path | `/FileStore/fraud-platform/silver/transactions` | Same pattern, different layer |
| Gold path | `/FileStore/fraud-platform/gold/` | Aggregated marts live here |
| Checkpoint path | `/FileStore/fraud-platform/checkpoints/` | Required by Spark Structured Streaming |
| Partition column (Bronze) | `date` (ingestion date) | Prune queries by date efficiently |
| Partition column (Silver) | `date`, `country_code` | Fraud analysis is almost always date + geography |
| Table format | Delta (not Parquet) | ACID, schema evolution, time travel, MERGE support |

---

## Schema Decisions

| Decision | Choice | Reason |
|----------|--------|--------|
| Transaction ID field | `transaction_id` (string UUID) | Universally unique, no integer overflow |
| Timestamp field | `event_timestamp` (Unix millis long) | Timezone-safe, Avro-compatible |
| Amount field | `amount_local` (double) + `amount_eur` (double) | Store original currency + normalised EUR |
| PII handling | `card_id` → SHA-256 hash in Silver | Never store raw card numbers in analytics layers |
| Avro namespace | `com.fraudplatform.payments` | Simulates real company namespace |

---

## dbt Decisions

| Decision | Choice | Reason |
|----------|--------|--------|
| dbt adapter | `dbt-databricks` | Connects dbt Core to Databricks CE |
| Materialisation default | `view` (staging), `table` (marts) | Views for staging = no storage cost; tables for marts = fast queries |
| Incremental strategy | `merge` on `transaction_id` | Idempotent — safe to re-run without duplicates |
| Test coverage | 100% on staging models | Non-negotiable in production data engineering |

---

## Naming Conventions (Fixed — never deviate)

```
Kafka topics:     raw.{entity}              e.g. raw.transactions
Dead-letter:      raw.{entity}.dead_letter  e.g. raw.transactions.dead_letter
Delta tables:     {layer}_{entity}          e.g. bronze_transactions
dbt staging:      stg_{entity}              e.g. stg_transactions
dbt intermediate: int_{description}         e.g. int_transaction_enriched
dbt marts:        mart_{description}        e.g. mart_fraud_daily_summary
Python files:     {layer}_{action}.py       e.g. bronze_ingestion.py
Airflow DAGs:     {project}_{schedule}.py   e.g. fraud_platform_pipeline.py
```

---
_Last updated: 2026-08-19_
