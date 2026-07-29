-- district_name を郡名・振興局名・政令市名に振り分けたとき、どれにも当たらない行を検出。
-- 結果が0行ならテスト成功。行が返る場合、上流に未知の語尾が現れたため
-- 振り分け条件が追随できていない可能性がある。
--
-- 「特別区部」(13100) は郡でも振興局でも政令市でもない既知の例外なので除外する。
SELECT
    area_code,
    pref_name,
    district_name,
    area_kind
FROM {{ ref('municipality') }}
WHERE district_name IS NOT NULL
    AND district_name <> '特別区部'
    AND county_name IS NULL
    AND subprefecture_name IS NULL
    AND designated_city_name IS NULL
