SELECT
    tab, cat01, cat02, cat03, cat04, area, time, unit, value,
    cat01_metadata, cat02_metadata, cat03_metadata, cat04_metadata,
    area_metadata, time_metadata
FROM {{ source('estat_source', 'labor_force_monthly') }}
