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
)

SELECT
    area_code,
    pref_code,
    pref_name,
    district_name,
    municipality_name,
    yomigana,
    is_prefecture,
    area_kind,
    -- 総務省が市区町村として数えるのは市区町村と政令指定都市。行政区は数えず、
    -- 郡・振興局・支庁・特別区部は集計行なので数えない。
    area_kind IN ('municipality', 'designated_city') AS is_municipality
FROM classified
