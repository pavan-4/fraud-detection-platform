-- models/intermediate/int_fraud_scored.sql
--
-- PURPOSE:
--   Computes fraud signals and risk score for every transaction.
--   This is the SQL equivalent of silver_enrichment.ipynb

WITH base AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

signals AS (
    SELECT
        *,

        -- Signal 1: Unusually high amount
        CASE WHEN amount_local > 500 THEN true ELSE false END       AS is_high_amount,

        -- Signal 2: Transaction outside Ireland (card home country)
        CASE WHEN country_code NOT IN ('IE') THEN true ELSE false END AS is_foreign_country,

        -- Signal 3: Suspicious merchant category
        CASE WHEN merchant_category_code IN ('7995','5933','6051')
             THEN true ELSE false END                                AS is_suspicious_mcc,

        -- Signal 4: Rapid succession — count txns per card in last 5 minutes
               COUNT(*) OVER (
            PARTITION BY card_id
            ORDER BY CAST(event_ts AS LONG)
            RANGE BETWEEN 300 PRECEDING AND CURRENT ROW
        )                                                            AS txn_count_5min

    FROM base
),

scored AS (
    SELECT
        *,
        CASE WHEN txn_count_5min > 2 THEN true ELSE false END       AS is_rapid_succession,

        -- Weighted risk score (0-100)
        (CASE WHEN amount_local > 500 THEN 40 ELSE 0 END) +
        (CASE WHEN country_code NOT IN ('IE') THEN 30 ELSE 0 END) +
        (CASE WHEN merchant_category_code IN ('7995','5933','6051') THEN 20 ELSE 0 END) +
        (CASE WHEN txn_count_5min > 2 THEN 10 ELSE 0 END)           AS risk_score

    FROM signals
),

labelled AS (
    SELECT
        *,
        CASE
            WHEN risk_score >= 60 THEN 'HIGH'
            WHEN risk_score >= 30 THEN 'MEDIUM'
            ELSE 'LOW'
        END                                                          AS risk_label

    FROM scored
)

SELECT * FROM labelled