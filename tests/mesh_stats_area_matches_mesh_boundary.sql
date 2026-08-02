-- メッシュ統計の area が境界データ mesh_1km と同じコード体系であることを検証する。
-- 地図化も空間集計も area = mesh_code の結合が前提なので、e-Stat 側がコードの
-- 桁数や体系を変えたら結合が静かに空振りする。統計値側に境界に無いメッシュが
-- 出たら落とす。
SELECT s.area
FROM (
    SELECT DISTINCT area FROM {{ ref('raw_mesh_population') }}
    UNION
    SELECT DISTINCT area FROM {{ ref('raw_mesh_establishment') }}
) AS s
LEFT JOIN {{ ref('mesh_1km') }} AS b ON b.mesh_code = s.area
WHERE b.mesh_code IS NULL
