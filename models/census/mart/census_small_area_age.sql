SELECT
    area,
    area_name,
    {{ e_stat_area_level('area') }} AS area_level,
    cat01,
    age_class,
    cat02,
    sex,
    unit,
    value
FROM {{ ref('stg_census_small_area_age') }}
