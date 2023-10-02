{{ config(materialized='table') }}


WITH

stg_chargeback_r AS (
  SELECT *
  FROM {{ ref('stg_chargeback_report') }}
),

transformed_acceptance_r AS (
  SELECT
    *,
    DATE(date_time) date,
    CASE 
        WHEN STATE = 'ACCEPTED' THEN TRUE
        WHEN STATE = 'DECLINED' THEN FALSE
    END AS is_accepted,
    AMOUNT/JSON_EXTRACT_PATH_TEXT(rates,currency) AS amount_in_usd
  FROM {{ ref('stg_acceptance_report') }}
),

joined AS (
  SELECT
    transformed_acceptance_r.external_ref,
    transformed_acceptance_r.status,
    transformed_acceptance_r.source,
    ref,
    date,
    is_accepted,
    cvv_provided,
    chargeback,
    country,
    amount_in_usd
  FROM transformed_acceptance_r
  LEFT JOIN stg_chargeback_r 
    ON transformed_acceptance_r.external_ref = stg_chargeback_r.external_ref
),

final AS (
  SELECT
    external_ref,
    status,
    source,
    ref,
    date,
    is_accepted,
    cvv_provided,
    chargeback,
    country,
    amount_in_usd
  FROM joined
)

SELECT * FROM final