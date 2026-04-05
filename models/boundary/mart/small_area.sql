SELECT
    key_code,
    prefecture_code,
    prefecture_name,
    city_code,
    city_name,
    area_name,
    kigo_e,
    kigo_d,
    kigo_i,
    jinko,
    setai,
    x_code,
    y_code,
    area_m2,
    geometry
FROM {{ ref('stg_census_boundary') }}
-- hcode 8101=町丁・字等, 8154=水面調査区
WHERE hcode = 8101
  -- 重複なし(NULL)または代表境界(E1)のみ残し、飛び地等の重複ポリゴン(E2以降)を除外
  AND (kigo_e IS NULL OR kigo_e = 'E1')
