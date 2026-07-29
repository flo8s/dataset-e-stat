-- municipality_code が階層区分と食い違っている行を検出。
-- 結果が0行ならテスト成功。

-- 市区町村として数える行なのに自分自身を指していない
SELECT
    'not_self_referencing' AS violation,
    area_code,
    municipality_code
FROM {{ ref('municipality') }}
WHERE is_municipality
    AND municipality_code IS DISTINCT FROM area_code

UNION ALL

-- 行政区なのに所属市が引けていない
SELECT
    'ward_without_municipality_code',
    area_code,
    municipality_code
FROM {{ ref('municipality') }}
WHERE area_kind = 'ward'
    AND municipality_code IS NULL

UNION ALL

-- 集計行（都道府県・郡・振興局・支庁・特別区部）なのに値が入っている
SELECT
    'aggregate_row_has_municipality_code',
    area_code,
    municipality_code
FROM {{ ref('municipality') }}
WHERE area_kind IN ('prefecture', 'district')
    AND municipality_code IS NOT NULL

UNION ALL

-- 指している先が市区町村として数える行になっていない
SELECT
    'points_to_non_municipality',
    m.area_code,
    m.municipality_code
FROM {{ ref('municipality') }} m
LEFT JOIN {{ ref('municipality') }} t
    ON t.area_code = m.municipality_code
    AND t.is_municipality
WHERE m.municipality_code IS NOT NULL
    AND t.area_code IS NULL
