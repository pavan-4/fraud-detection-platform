-- models/staging/stg_transactions.sql
--
-- PURPOSE:
--   First model in the pipeline. Reads raw Bronze transactions,
--   casts types properly, and standardises column names.
--   Everything downstream builds on this.

WITH source AS (
    SELECT * FROM workspace.fraud_platform.`bronze_transactions`
),

staged AS (
    SELECT
        transaction_id,
        card_id,
        customer_id,
        merchant_id,
        merchant_name,
        merchant_category_code,

        CAST(amount_local   AS DECIMAL(12,2))   AS amount_local,
        currency_code,
        CAST(amount_eur     AS DECIMAL(12,2))   AS amount_eur,
        country_code,
        terminal_type,

        event_ts,
        ingested_at,
        source_topic,

        -- Fraud simulation label (NULL on normal transactions)
        _fraud_simulation_type                  AS fraud_simulation_type,

        -- Kafka metadata kept for debugging/replay
        kafka_partition,
        kafka_offset

    FROM source
    WHERE transaction_id IS NOT NULL   -- drop malformed rows
)

SELECT * FROM staged