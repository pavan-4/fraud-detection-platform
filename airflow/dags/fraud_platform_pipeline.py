# airflow/dags/fraud_platform_pipeline.py
#
# PURPOSE:
#   Orchestrates the full fraud detection pipeline end-to-end.
#   Runs every 5 minutes:
#     1. Ingest new transactions from Event Hubs → Bronze Delta table
#     2. Recompute fraud signals → Silver Delta table
#     3. Rebuild fraud alert queue → Gold Delta table
#     4. Rebuild dbt mart_fraud_alerts table
#     5. Run dbt data quality tests
#
# HOW TO VIEW:
#   Open http://localhost:8088 → login admin/admin → find fraud_platform_pipeline

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.operators.bash import BashOperator

# ── DAG DEFAULT ARGUMENTS ────────────────────────────────────────────────────
default_args = {
    "owner":            "pavan",
    "depends_on_past":  False,
    "email_on_failure": False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=1),
}

# ── NOTEBOOK PATHS IN DATABRICKS WORKSPACE ───────────────────────────────────
BRONZE_NOTEBOOK = "/Users/pavankumar.ga14@gmail.com/bronze_ingestion"
SILVER_NOTEBOOK = "/Users/pavankumar.ga14@gmail.com/silver_enrichment"
GOLD_NOTEBOOK   = "/Users/pavankumar.ga14@gmail.com/gold_fraud_alerts"

# ── DAG DEFINITION ────────────────────────────────────────────────────────────
with DAG(
    dag_id="fraud_platform_pipeline",
    description="End-to-end fraud detection: Event Hubs → Bronze → Silver → Gold → dbt",
    default_args=default_args,
    schedule_interval="*/5 * * * *",   # Every 5 minutes
    start_date=datetime(2026, 8, 21),
    catchup=False,
    tags=["fraud", "databricks", "dbt"],
) as dag:

    # ── TASK 1: Bronze Ingestion ──────────────────────────────────────────────
    # Multi-task format required for serverless compute (Databricks Free Edition)
    run_bronze = DatabricksSubmitRunOperator(
        task_id="bronze_ingestion",
        databricks_conn_id="databricks_default",
        json={
            "run_name": "airflow_bronze_ingestion",
            "tasks": [
                {
                    "task_key": "bronze_ingestion",
                    "notebook_task": {
                        "notebook_path": BRONZE_NOTEBOOK,
                        "base_parameters": {}
                    }
                }
            ],
            "queue": {"enabled": True}
        },
    )

    # ── TASK 2: Silver Enrichment ─────────────────────────────────────────────
    run_silver = DatabricksSubmitRunOperator(
        task_id="silver_enrichment",
        databricks_conn_id="databricks_default",
        json={
            "run_name": "airflow_silver_enrichment",
            "tasks": [
                {
                    "task_key": "silver_enrichment",
                    "notebook_task": {
                        "notebook_path": SILVER_NOTEBOOK,
                        "base_parameters": {}
                    }
                }
            ],
            "queue": {"enabled": True}
        },
    )

    # ── TASK 3: Gold Fraud Alerts ─────────────────────────────────────────────
    run_gold = DatabricksSubmitRunOperator(
        task_id="gold_fraud_alerts",
        databricks_conn_id="databricks_default",
        json={
            "run_name": "airflow_gold_fraud_alerts",
            "tasks": [
                {
                    "task_key": "gold_fraud_alerts",
                    "notebook_task": {
                        "notebook_path": GOLD_NOTEBOOK,
                        "base_parameters": {}
                    }
                }
            ],
            "queue": {"enabled": True}
        },
    )

    # ── TASK 4: dbt Run ───────────────────────────────────────────────────────
    run_dbt = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/dbt && dbt run --profiles-dir /home/airflow/.dbt",
    )

    # ── TASK 5: dbt Test ──────────────────────────────────────────────────────
    test_dbt = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/dbt && dbt test --profiles-dir /home/airflow/.dbt",
    )

    # ── PIPELINE ORDER ────────────────────────────────────────────────────────
    run_bronze >> run_silver >> run_gold >> run_dbt >> test_dbt
