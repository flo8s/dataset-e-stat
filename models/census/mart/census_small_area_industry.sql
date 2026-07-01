SELECT
    area,
    area_name,
    cat01,
    industry,
    cat02,
    secrecy,
    unit,
    value
FROM {{ ref('stg_census_small_area_industry') }}
