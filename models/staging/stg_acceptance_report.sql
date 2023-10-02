{{ config(materialized='view') }}

SELECT
    *
FROM {{ source('globe_api', 'acceptance_report') }}
