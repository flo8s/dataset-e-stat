SELECT
    tab, cat01, cat02, area, time, unit, value,
    cat01_metadata, cat02_metadata, area_metadata, time_metadata
FROM {{ source('estat_source', 'household_quantity_monthly') }}
