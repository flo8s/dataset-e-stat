SELECT
    effective_date,
    pref_code,
    pref_name,
    old_code,
    old_name,
    new_code,
    new_name,
    is_code_deleted,
    reason
FROM {{ ref('stg_municipality_change') }}
