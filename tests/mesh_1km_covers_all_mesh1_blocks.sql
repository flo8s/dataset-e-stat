-- 1kmメッシュ境界が配布単位（1次メッシュ）を全て含むことを検証する。
-- 統計GIS の配布一覧は 176 区画。ダウンロードが部分的に失敗しても
-- ビルドは通ってしまうため、区画数が欠けていればテストで落とす。
SELECT COUNT(DISTINCT mesh1_code) AS mesh1_blocks
FROM {{ ref('mesh_1km') }}
HAVING COUNT(DISTINCT mesh1_code) <> 176
