-- stats_list に存在するが stats_field (seed) にない分野コードを検出。
-- 結果が0行ならテスト成功。行が返る場合、統計分野体系が更新された可能性がある。
SELECT DISTINCT
    s.main_category_code,
    s.main_category,
    s.sub_category_code,
    s.sub_category
FROM {{ ref('stg_stats_list') }} s
LEFT JOIN {{ ref('seed_stats_field') }} f
    ON s.main_category_code = f.main_category_code
    AND s.sub_category_code = f.sub_category_code
WHERE f.main_category_code IS NULL
