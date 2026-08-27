-- 就業状態等基本集計の 2 表で、粒度が二重計上なく全国を覆うことを検証する。
-- 結果が0行ならテスト成功。
--
-- area には全国・都道府県・市・特別区部・政令指定都市の区・特別区・町村が縦に
-- 並んでいる。日本全域をちょうど1回覆うのは prefecture だけ、および
-- city + town_village の組だけで、ward はその内訳にあたる。area_level の
-- 切り分けを間違えると例えば特別区が city に混ざり、東京都の値が二重に乗る。
-- 男女・区分ごとに全国と一致することでその切り分けを担保する。

WITH labor_force AS (
    SELECT
        'census_municipality_labor_force' AS model_name,
        sex_code,
        labor_status_code AS category_code,
        SUM(value) FILTER (WHERE area_level = 'national') AS national,
        SUM(value) FILTER (WHERE area_level = 'prefecture') AS prefecture,
        SUM(value) FILTER (WHERE area_level IN ('city', 'town_village')) AS municipality
    FROM {{ ref('census_municipality_labor_force') }}
    GROUP BY sex_code, labor_status_code
),

employment_status AS (
    SELECT
        'census_municipality_employment_status' AS model_name,
        sex_code,
        employment_status_code AS category_code,
        SUM(value) FILTER (WHERE area_level = 'national') AS national,
        SUM(value) FILTER (WHERE area_level = 'prefecture') AS prefecture,
        SUM(value) FILTER (WHERE area_level IN ('city', 'town_village')) AS municipality
    FROM {{ ref('census_municipality_employment_status') }}
    GROUP BY sex_code, employment_status_code
)

SELECT * FROM (
    SELECT * FROM labor_force
    UNION ALL
    SELECT * FROM employment_status
)
WHERE prefecture <> national OR municipality <> national
