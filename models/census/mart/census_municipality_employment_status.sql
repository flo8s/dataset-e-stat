SELECT
    area,
    area_name,
    area_level,
    parent_area,
    cat01 AS sex_code,
    sex,
    cat02 AS employment_status_code,
    employment_status,
    employment_status_level,
    is_reprint,
    unit,
    value
FROM {{ ref('stg_census_municipality_employment_status') }}
