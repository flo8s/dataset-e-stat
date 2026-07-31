SELECT
    area,
    area_name,
    {{ e_stat_area_level('area') }} AS area_level,
    cat01,
    family_type,
    cat02,
    secrecy,
    unit,
    value
FROM {{ ref('stg_census_small_area_household') }}
