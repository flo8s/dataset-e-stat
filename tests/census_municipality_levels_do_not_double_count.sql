-- census_municipality の粒度が二重計上なく全国を覆うことを検証する。
-- 結果が0行ならテスト成功。
--
-- area には全国・都道府県・市区町村・区・旧市区町村が縦に並んでいる。日本全域を
-- ちょうど1回覆うのは prefecture だけ、および city + town_village の組だけで、
-- ward(政令指定都市の区・特別区)は city の内訳、former_municipality(2000年の
-- 市区町村)は現行市区町村の再掲にあたる。area_level の切り分けを間違えると
-- 例えば特別区が city に混ざり、東京都の人口が二重に乗る。
-- 人口が一致することでその切り分けを担保する。

WITH totals AS (
    SELECT
        SUM(population) FILTER (WHERE area_level = 'national') AS national,
        SUM(population) FILTER (WHERE area_level = 'prefecture') AS prefecture,
        SUM(population) FILTER (WHERE area_level IN ('city', 'town_village')) AS municipality
    FROM {{ ref('census_municipality') }}
)
SELECT * FROM totals
WHERE prefecture <> national OR municipality <> national
