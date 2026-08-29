-- 年次の支出金額が同じ年の月次12か月の合計と一致することを検証する。
-- 結果が0行ならテスト成功。
--
-- 年次は年平均ではなく12か月の合計で、月次と年次を同じ表に積んである以上、
-- どちらなのかを間違えると12倍ずれた記述になる。年次表と月次表は別の統計表 ID から
-- 取っていて、片方が別の表に差し替わっても行数も列も変わらないまま通ってしまう。
--
-- 単位が円でない品目（世帯人員・持家率など）は年次が年平均なので対象外にする。
-- 月次が12か月そろっていない年（最新年や、県庁所在市の収録開始年）も対象外。
-- 各月の値は円未満で丸められているので、合計は年次の値と数円ずれうる。
--
-- 突き合わせだけだと年次側が丸ごと消えたときに結合が 0 行になって成功してしまうので、
-- 月次が12か月そろっている世帯区分・地域に年次の行が1つも無い場合も落とす。

WITH monthly AS (
    SELECT cat01, cat02, area, year, COUNT(value) AS month_count, SUM(value) AS monthly_total
    FROM {{ ref('expenditure') }}
    WHERE frequency = '月次' AND unit = '円'
    GROUP BY cat01, cat02, area, year
),
complete_monthly AS (
    SELECT * FROM monthly WHERE month_count = 12
),
annual AS (
    SELECT cat01, cat02, area, year, value AS annual_value
    FROM {{ ref('expenditure') }}
    WHERE frequency = '年次' AND unit = '円' AND value IS NOT NULL
)
SELECT 'value_mismatch' AS check_name, a.cat02, a.area, CAST(a.year AS VARCHAR) AS year, a.cat01
FROM annual a
JOIN complete_monthly m
  ON a.cat01 = m.cat01 AND a.cat02 = m.cat02 AND a.area = m.area AND a.year = m.year
WHERE ABS(a.annual_value - m.monthly_total) > 10

UNION ALL

SELECT 'annual_missing' AS check_name, m.cat02, m.area, NULL AS year, NULL AS cat01
FROM complete_monthly m
WHERE NOT EXISTS (
    SELECT 1 FROM annual a WHERE a.cat02 = m.cat02 AND a.area = m.area
)
GROUP BY m.cat02, m.area
