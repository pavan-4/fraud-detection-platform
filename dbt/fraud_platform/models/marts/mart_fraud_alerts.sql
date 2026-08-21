-- models/marts/mart_fraud_alerts.sql
--
-- PURPOSE:
--   Final Gold table. One row per HIGH risk transaction.
--   This is what a fraud analyst sees in their queue.
--   Built as a TABLE (not view) so dashboards query fast.

{{ config(materialized='table') }}

WITH scored AS (
    SELECT * FROM {{ ref('int_fraud_scored') }}
),

alerts AS (
    SELECT
        transaction_id,
        card_id,
        customer_id,
        event_ts,
        merchant_name,
        merchant_category_code,
        amount_local,
        currency_code,
        country_code,
        terminal_type,
        risk_score,
        risk_label,
        is_high_amount,
        is_foreign_country,
        is_suspicious_mcc,
        is_rapid_succession,
        txn_count_5min,
        fraud_simulation_type,
        ingested_at,

        -- Human readable reason for the alert
        CONCAT_WS(' + ',
            CASE WHEN is_high_amount      THEN 'HIGH_AMOUNT'      END,
            CASE WHEN is_foreign_country  THEN 'FOREIGN_COUNTRY'  END,
            CASE WHEN is_suspicious_mcc   THEN 'SUSPICIOUS_MCC'   END,
            CASE WHEN is_rapid_succession THEN 'RAPID_SUCCESSION' END
        )                               AS alert_reason,

        CURRENT_TIMESTAMP()             AS mart_built_at

    FROM scored
    WHERE risk_label = 'HIGH'
)

SELECT * FROM alerts
ORDER BY risk_score DESC, event_ts DESC