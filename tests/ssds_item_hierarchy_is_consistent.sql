-- stg_item_lookup が組み立てる階層が壊れていないことを検証する。
-- 結果が0行ならテスト成功。
--
-- 階層は item_catalog の level / parent_code が使えない（level は全て 1、
-- parent_code は全て空）ため、コードの前方一致から導いている。導出である以上、
-- 上流のコード体系が変われば黙って壊れる。以下の3つが崩れたら気づけるようにする。
--
--   self_parent   親が自分自身
--   orphan        存在しないコードを親として指している
--   level_skew    level が親の level + 1 になっていない

SELECT 'self_parent' AS violation, item_code, parent_code, level
FROM {{ ref('stg_item_lookup') }}
WHERE parent_code = item_code

UNION ALL

SELECT 'orphan', c.item_code, c.parent_code, c.level
FROM {{ ref('stg_item_lookup') }} c
WHERE c.parent_code IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM {{ ref('stg_item_lookup') }} p
      WHERE p.item_code = c.parent_code
  )

UNION ALL

SELECT 'level_skew', c.item_code, c.parent_code, c.level
FROM {{ ref('stg_item_lookup') }} c
JOIN {{ ref('stg_item_lookup') }} p ON c.parent_code = p.item_code
WHERE c.level <> p.level + 1
