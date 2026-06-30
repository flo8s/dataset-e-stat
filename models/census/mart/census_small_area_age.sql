SELECT
    area,
    area_name,
    cat01,
    age_class,
    cat02,
    sex,
    unit,
    value
FROM {{ ref('stg_census_small_area_age') }}
