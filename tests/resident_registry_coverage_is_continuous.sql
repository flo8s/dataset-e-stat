-- population の収録が年次で途切れず、住民区分と調査期日の切り替わりが
-- 想定どおりであることを検証する。結果が0行ならテスト成功。
--
-- 取得はカタログが返した統計表を年ごとに選ぶ形なので、名前の付け方が変わって
-- 拾えなかった年があっても、その年が欠けたままビルドは通ってしまう。

-- 収録年が途切れている
SELECT
    'missing_year' AS violation,
    y AS year,
    NULL AS detail
FROM range(
    (SELECT MIN(year) FROM {{ ref('population') }}),
    (SELECT MAX(year) + 1 FROM {{ ref('population') }})
) t(y)
WHERE y NOT IN (SELECT year FROM {{ ref('population') }})

UNION ALL

-- 2013年以降なのに総計・日本人住民・外国人住民が揃っていない
SELECT
    'incomplete_resident_kind',
    year,
    CAST(COUNT(DISTINCT resident_kind) AS VARCHAR)
FROM {{ ref('population') }}
WHERE year >= 2013
GROUP BY year
HAVING COUNT(DISTINCT resident_kind) <> 3

UNION ALL

-- 2012年以前は住民基本台帳が日本人住民のみを対象とするので japanese だけになる
SELECT
    'unexpected_resident_kind',
    year,
    resident_kind
FROM {{ ref('population') }}
WHERE year <= 2012 AND resident_kind <> 'japanese'
GROUP BY year, resident_kind

UNION ALL

-- 調査期日は2014年以降が1月1日、2013年以前が3月31日
SELECT
    'unexpected_reference_date',
    year,
    CAST(reference_date AS VARCHAR)
FROM {{ ref('population') }}
WHERE (year >= 2014 AND (MONTH(reference_date), DAY(reference_date)) <> (1, 1))
    OR (year <= 2013 AND (MONTH(reference_date), DAY(reference_date)) <> (3, 31))
GROUP BY year, reference_date
