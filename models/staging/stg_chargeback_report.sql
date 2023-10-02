{{ config(materialized='view') }}

SELECT
    external_ref,
    chargeback
FROM {{ source('globe_api', 'chargeback_report') }}
