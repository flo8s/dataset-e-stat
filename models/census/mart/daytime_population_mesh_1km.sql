SELECT
    mesh_code,
    nighttime_population,
    daytime_population,
    CASE
        WHEN nighttime_population > 0
            THEN ROUND(daytime_population * 100.0 / nighttime_population, 1)
    END AS daytime_ratio,
    daytime_population - nighttime_population AS daytime_night_diff,
    daytime_population_uncalibrated,
    residents_working_or_studying,
    students_commuting_out,
    employed_residents,
    primary_industry_residents,
    non_labor_force,
    unemployed,
    preschool_children,
    resident_students,
    resident_students_compulsory,
    establishments,
    employees_total,
    employees_secondary,
    employees_tertiary
FROM {{ ref('stg_mesh_daytime_population') }}
