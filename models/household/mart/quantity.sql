SELECT
    cat01,
    item_name,
    item_level,
    item_parent,
    cat02,
    household_type,
    area,
    area_name,
    time,
    time_name,
    frequency,
    year,
    month,
    unit,
    value
FROM {{ ref('stg_household_quantity') }}
