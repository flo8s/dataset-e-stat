SELECT
    area,
    area_name,
    area_level,
    parent_area,
    cat01 AS sex_code,
    sex,
    cat03 AS labor_status_code,
    labor_status,
    labor_status_level,
    unit,
    value
FROM {{ ref('stg_census_municipality_labor_force') }}
