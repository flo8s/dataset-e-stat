-- establishment_industry の粒度が二重計上なく全国を覆うことを検証する。
-- 結果が0行ならテスト成功。
--
-- area には全国・都道府県・市区町村・特別区部・行政区が縦に並んでいる。日本全域を
-- ちょうど1回覆うのは prefecture だけ、および municipality だけで、
-- ward(政令指定都市の行政区・特別区・境界未定地域)は municipality の内訳にあたる。
-- 特別区は ward で、その親「特別区部」(13100)が municipality に立つ。
-- area_level の切り分けを間違えると特別区が municipality に混ざり、東京の事業所が
-- 二重に乗る。全産業・経営組織の総数で合計が一致することでその切り分けを担保する。

WITH totals AS (
    SELECT
        SUM(establishments) FILTER (WHERE area_level = 'national') AS national_establishments,
        SUM(establishments) FILTER (WHERE area_level = 'prefecture') AS prefecture_establishments,
        SUM(establishments) FILTER (WHERE area_level = 'municipality') AS municipality_establishments,
        SUM(employees) FILTER (WHERE area_level = 'national') AS national_employees,
        SUM(employees) FILTER (WHERE area_level = 'prefecture') AS prefecture_employees,
        SUM(employees) FILTER (WHERE area_level = 'municipality') AS municipality_employees
    FROM {{ ref('establishment_industry') }}
    WHERE industry_code = 'AR' AND organization_code = '0'
)
SELECT * FROM totals
WHERE prefecture_establishments <> national_establishments
   OR municipality_establishments <> national_establishments
   OR prefecture_employees <> national_employees
   OR municipality_employees <> national_employees
