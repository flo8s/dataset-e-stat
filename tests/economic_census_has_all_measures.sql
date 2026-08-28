-- establishment_industry の測定項目が全て埋まっていることを検証する。
-- 結果が0行ならテスト成功。
--
-- mart は原典の表章項目コード（102-2021 など）で列を開いている。e-Stat がコードを
-- 振り直すと該当する列が丸ごと NULL になるが、行数も area_level も変わらないので
-- 他のテストでは気づけない。
--
-- 売上（収入）金額は産業によって調査されていないため、全産業では必ず NULL になる。
-- 6項目すべてが揃うのは売上まで調査されている産業で、全国・卸売業，小売業・経営組織の
-- 総数を代表に見る。事業所数と従業者数は全産業で揃う。
--
-- 代表行が1件あることも併せて見る。産業や経営組織のコードが振り直されて代表行その
-- ものが消えると、NULL を探す条件は0行 = 合格を返し、狙っている壊れ方を素通りする。

SELECT '全6項目が揃う代表行（全国・卸売業，小売業・総数）が1件でない' AS violation
FROM {{ ref('establishment_industry') }}
WHERE area = '00000' AND industry_code = 'I' AND organization_code = '0'
HAVING COUNT(*) <> 1
UNION ALL
SELECT '代表行に欠けている測定項目がある'
FROM {{ ref('establishment_industry') }}
WHERE area = '00000' AND industry_code = 'I' AND organization_code = '0'
  AND (
    establishments IS NULL
    OR employees IS NULL
    OR sales_million_yen IS NULL
    OR employees_per_establishment IS NULL
    OR sales_per_establishment_10k_yen IS NULL
    OR sales_per_employee_10k_yen IS NULL
  )
UNION ALL
SELECT '全国の全産業の行が1件でない'
FROM {{ ref('establishment_industry') }}
WHERE area = '00000' AND industry_code = 'AR' AND organization_code = '0'
HAVING COUNT(*) <> 1
UNION ALL
SELECT '全国の全産業で事業所数か従業者数が欠けている'
FROM {{ ref('establishment_industry') }}
WHERE area = '00000' AND industry_code = 'AR' AND organization_code = '0'
  AND (establishments IS NULL OR employees IS NULL)
