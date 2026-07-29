-- 階層区分は district_name の NULL を COALESCE で潰してから判定する。
-- district_name が NULL の行が 795 件（郡に属さない市 772 + 特別区 23）あり、
-- NOT (...) や CASE の分岐条件に素のまま渡すとこの 795 件が丸ごと落ちる。
WITH classified AS (
    SELECT
        area_code,
        pref_code,
        pref_name,
        district_name,
        municipality_name,
        yomigana,
        is_prefecture,
        CASE
            WHEN is_prefecture THEN 'prefecture'
            WHEN municipality_name IS NULL AND COALESCE(district_name, '') LIKE '%市' THEN 'designated_city'
            WHEN municipality_name IS NULL THEN 'district'
            WHEN COALESCE(district_name, '') LIKE '%市' THEN 'ward'
            ELSE 'municipality'
        END AS area_kind
    FROM {{ ref('stg_municipality') }}
),

-- district_name には郡名・振興局名・支庁名・政令市名・「特別区部」が同居している。
-- 語尾で振り分けて別列にする。「特別区部」(13100) はどれにも当たらない唯一の行。
split_names AS (
    SELECT
        *,
        CASE WHEN district_name LIKE '%郡' THEN district_name END AS county_name,
        CASE
            WHEN district_name LIKE '%振興局' OR district_name LIKE '%支庁'
                THEN district_name
        END AS subprefecture_name,
        CASE WHEN district_name LIKE '%市' THEN district_name END AS designated_city_name
    FROM classified
)

SELECT
    s.area_code,
    s.pref_code,
    s.pref_name,
    s.district_name,
    s.municipality_name,
    s.yomigana,
    s.is_prefecture,
    s.area_kind,
    -- 総務省が市区町村として数えるのは市区町村と政令指定都市。行政区は数えず、
    -- 郡・振興局・支庁・特別区部は集計行なので数えない。
    s.area_kind IN ('municipality', 'designated_city') AS is_municipality,
    s.county_name,
    s.subprefecture_name,
    s.designated_city_name,
    -- 行政区はその市のコードを指し、市区町村と政令指定都市は自分自身を指す。
    -- 特別区は基礎自治体なので自分自身（親の「特別区部」ではない）。
    CASE
        WHEN s.area_kind = 'ward' THEN p.area_code
        WHEN s.area_kind IN ('municipality', 'designated_city') THEN s.area_code
    END AS municipality_code
FROM split_names s
LEFT JOIN split_names p
    ON s.area_kind = 'ward'
    AND p.area_kind = 'designated_city'
    AND p.pref_code = s.pref_code
    AND p.district_name = s.district_name
