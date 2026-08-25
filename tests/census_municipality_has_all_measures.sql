-- census_municipality の測定項目が全て埋まっていることを、全国行で検証する。
-- 結果が0行ならテスト成功。
--
-- mart は原典の表章事項コード（2020_13 など）と男女コード（0/1/2）で列を開いている。
-- e-Stat がコードを振り直すと該当する列が丸ごと NULL になるが、行数も area_level も
-- 変わらないので他のテストでは気づけない。全国行は必ず全項目に値があるので、
-- ここで落とす。

SELECT *
FROM {{ ref('census_municipality') }}
WHERE area_level = 'national'
  AND (
    population IS NULL
    OR population_male IS NULL
    OR population_female IS NULL
    OR households IS NULL
    OR households_general IS NULL
    OR households_institutional IS NULL
    OR household_members IS NULL
    OR household_members_general IS NULL
    OR household_members_institutional IS NULL
    OR population_2015 IS NULL
    OR households_2015 IS NULL
    OR population_change_5y IS NULL
    OR population_change_rate_5y IS NULL
    OR household_change_5y IS NULL
    OR household_change_rate_5y IS NULL
    OR sex_ratio IS NULL
    OR area_km2 IS NULL
    OR population_density IS NULL
  )
