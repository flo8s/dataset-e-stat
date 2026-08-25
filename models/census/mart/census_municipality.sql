{# 3 つの統計表(1-1-1 / 1-1-2 / 1-1-3)は area の分類が完全に一致する(4,086 地域)。
   測定項目は表ごとに固定なので、縦持ちのままにせず地域 1 行の横持ちに開く。
   縦持ちだと「総数」と内訳、人と世帯と％ が同じ列に並び、絞り込みを忘れた
   合計が黙って通る。 #}
WITH population AS (
    SELECT
        area,
        ANY_VALUE(area_name) AS area_name,
        ANY_VALUE(area_level) AS area_level,
        ANY_VALUE(parent_area) AS parent_area,
        MAX(value) FILTER (WHERE cat01 = '0') AS population,
        MAX(value) FILTER (WHERE cat01 = '1') AS population_male,
        MAX(value) FILTER (WHERE cat01 = '2') AS population_female
    FROM {{ ref('stg_census_municipality_population') }}
    GROUP BY area
),

household AS (
    SELECT
        area,
        MAX(value) FILTER (WHERE tab = '2020_13' AND cat01 = '0') AS households,
        MAX(value) FILTER (WHERE tab = '2020_13' AND cat01 = '1') AS households_general,
        MAX(value) FILTER (WHERE tab = '2020_13' AND cat01 = '2') AS households_institutional,
        MAX(value) FILTER (WHERE tab = '2020_22' AND cat01 = '0') AS household_members,
        MAX(value) FILTER (WHERE tab = '2020_22' AND cat01 = '1') AS household_members_general,
        MAX(value) FILTER (WHERE tab = '2020_22' AND cat01 = '2') AS household_members_institutional
    FROM {{ ref('stg_census_municipality_household') }}
    GROUP BY area
),

change AS (
    SELECT
        area,
        CAST(MAX(value) FILTER (WHERE tab = '2020_03') AS BIGINT) AS population_2015,
        CAST(MAX(value) FILTER (WHERE tab = '2020_15') AS BIGINT) AS households_2015,
        CAST(MAX(value) FILTER (WHERE tab = '2020_34') AS BIGINT) AS population_change_5y,
        MAX(value) FILTER (WHERE tab = '2020_35') AS population_change_rate_5y,
        CAST(MAX(value) FILTER (WHERE tab = '2020_36') AS BIGINT) AS household_change_5y,
        MAX(value) FILTER (WHERE tab = '2020_37') AS household_change_rate_5y,
        MAX(value) FILTER (WHERE tab = '2020_46') AS sex_ratio,
        MAX(value) FILTER (WHERE tab = '2020_47') AS area_km2,
        MAX(value) FILTER (WHERE tab = '2020_48') AS population_density
    FROM {{ ref('stg_census_municipality_change') }}
    GROUP BY area
)

SELECT
    p.area,
    p.area_name,
    p.area_level,
    p.parent_area,
    p.population,
    p.population_male,
    p.population_female,
    h.households,
    h.households_general,
    h.households_institutional,
    h.household_members,
    h.household_members_general,
    h.household_members_institutional,
    c.population_2015,
    c.households_2015,
    c.population_change_5y,
    c.population_change_rate_5y,
    c.household_change_5y,
    c.household_change_rate_5y,
    c.sex_ratio,
    c.area_km2,
    c.population_density
FROM population p
LEFT JOIN household h ON p.area = h.area
LEFT JOIN change c ON p.area = c.area
