{{
    config(
        materialized = 'table',
    )
}}

SELECT UNNEST(GENERATE_SERIES(DATE '2000-01-01', DATE '2030-12-31', INTERVAL '1 day'))::DATE AS date_day
