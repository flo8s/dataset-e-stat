-- area_kind による階層区分が上流データの構造と食い違っていないかを検証する。
-- 結果が0行ならテスト成功。行が返る場合、標準地域コードの構成が変わったか
-- 判定条件が上流の表記変更に追随できていない可能性がある。
--
-- 件数そのものは市町村合併や区の再編で正当に変わるため固定しない。
-- 都道府県数だけは法定で不変なので検査する。

-- 行政区に親の政令指定都市が引けない
SELECT
    'ward_without_designated_city' AS violation,
    w.area_code,
    w.district_name AS detail
FROM {{ ref('municipality') }} w
LEFT JOIN {{ ref('municipality') }} p
    ON p.pref_code = w.pref_code
    AND p.district_name = w.district_name
    AND p.area_kind = 'designated_city'
WHERE w.area_kind = 'ward'
    AND p.area_code IS NULL

UNION ALL

-- 政令指定都市に配下の行政区が1つも無い
SELECT
    'designated_city_without_ward',
    c.area_code,
    c.district_name
FROM {{ ref('municipality') }} c
WHERE c.area_kind = 'designated_city'
    AND NOT EXISTS (
        SELECT 1
        FROM {{ ref('municipality') }} w
        WHERE w.area_kind = 'ward'
            AND w.pref_code = c.pref_code
            AND w.district_name = c.district_name
    )

UNION ALL

-- 郡・振興局・支庁に属する市区町村なのに、その親の集計行が引けない
SELECT
    'municipality_without_district',
    m.area_code,
    m.district_name
FROM {{ ref('municipality') }} m
LEFT JOIN {{ ref('municipality') }} p
    ON p.pref_code = m.pref_code
    AND p.district_name = m.district_name
    AND p.area_kind = 'district'
WHERE m.area_kind = 'municipality'
    AND m.district_name IS NOT NULL
    AND p.area_code IS NULL

UNION ALL

-- 都道府県が47件でない
SELECT
    'prefecture_count_changed',
    CAST(COUNT(*) AS VARCHAR),
    NULL
FROM {{ ref('municipality') }}
WHERE area_kind = 'prefecture'
HAVING COUNT(*) <> 47
