-- メッシュ統計が全47都道府県分そろっていることを検証する。
-- 統計GIS の CSV は都道府県別配布で、pipelines/mesh_stats.py は取得済みファイルを
-- スキップするため、途中で失敗したまま次回もスキップされると欠けたまま build が
-- 通ってしまう。表ごとに 47 件そろっているかを見る。
WITH counts AS (
    SELECT 'raw_mesh_population' AS model, COUNT(DISTINCT pref_code) AS prefs
    FROM {{ ref('raw_mesh_population') }}
    UNION ALL
    SELECT 'raw_mesh_labor', COUNT(DISTINCT pref_code)
    FROM {{ ref('raw_mesh_labor') }}
    UNION ALL
    SELECT 'raw_mesh_commute', COUNT(DISTINCT pref_code)
    FROM {{ ref('raw_mesh_commute') }}
    UNION ALL
    SELECT 'raw_mesh_establishment', COUNT(DISTINCT pref_code)
    FROM {{ ref('raw_mesh_establishment') }}
)

SELECT model, prefs
FROM counts
WHERE prefs <> 47
