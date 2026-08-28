SELECT
    tab, cat01, cat02, area, value,
    cat01_metadata, cat02_metadata, area_metadata
FROM {{ source('estat_source', 'economic_census_industry') }}
