-- メッシュ統計が配布単位である 1次メッシュを全て含むことを検証する。
-- 統計値は 1次メッシュごとに別の統計表として配布され、pipelines/mesh_stats.py が
-- statsDataId を1件ずつ取得して merge する。一部の区画だけ取得に失敗しても
-- merge は成功し、前回ロード分が残るので行数の減少としても現れない。
-- 区画数が欠けていればテストで落とす。
--
-- 期待値をベタ書きしているのは、MESH_STATS_TABLES が STATISTICS_NAME で調査年を
-- 固定しているため。市区町村数と違って現実の出来事で勝手に変わる値ではなく、
-- 対象の調査年を変えたときだけ人が書き換える。
-- 令和2年国勢調査(1kmメッシュ) = 151区画 / 平成28年経済センサス(1kmメッシュ) = 149区画。
-- 経済センサス側が2区画少ないのは 3653 と 3741 に事業所が無いためで、
-- 区画集合は国勢調査側の部分集合になる。
WITH counts AS (
    SELECT
        'raw_mesh_population' AS model,
        COUNT(DISTINCT SUBSTR(area, 1, 4)) AS mesh1_blocks,
        151 AS expected
    FROM {{ ref('raw_mesh_population') }}

    UNION ALL

    SELECT
        'raw_mesh_establishment',
        COUNT(DISTINCT SUBSTR(area, 1, 4)),
        149
    FROM {{ ref('raw_mesh_establishment') }}
)

SELECT model, mesh1_blocks, expected
FROM counts
WHERE mesh1_blocks <> expected
