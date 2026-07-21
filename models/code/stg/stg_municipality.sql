SELECT
    area_code,
    pref_code,
    pref_name,
    district_name,
    municipality_name,
    yomigana,
    is_prefecture
FROM {{ ref('raw_municipality') }}
