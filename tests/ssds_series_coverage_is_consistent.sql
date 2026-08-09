-- series_coverage の各行が収録年として読める形をしていることを検証する。
-- 結果が0行ならテスト成功。
--
-- 収録年が読めない指標（year が NULL、value がすべて NULL）は集計側の WHERE で
-- 落としているので、ここに行が返るのは落とし方が壊れたときだけ。
SELECT table_name, cat01, min_year, max_year, year_count
FROM {{ ref('series_coverage') }}
WHERE min_year IS NULL
   OR max_year IS NULL
   OR min_year > max_year
   OR year_count < 1
