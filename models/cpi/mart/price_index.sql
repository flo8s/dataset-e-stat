SELECT
    tab,
    tab_name,
    cat01,
    item_name,
    area,
    area_name,
    time,
    time_name,
    year,
    unit,
    value
FROM {{ ref('stg_cpi') }}
