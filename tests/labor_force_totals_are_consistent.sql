-- 労働力調査の就業状態の恒等式を検証する。結果が0行ならテスト成功。
--
-- 労働力人口 = 就業者 + 完全失業者、就業者 = 従業者 + 休業者。
-- 就業状態は総数と内訳が同じ列に縦に積まれていて、labor_status_level だけでは
-- どれが誰の内訳かが読めない。恒等式が成り立つ組み合わせをここで固定しておかないと、
-- e-Stat 側でコードの意味が入れ替わっても行数も level も変わらないまま通ってしまう。
--
-- 15歳以上人口 = 労働力人口 + 非労働力人口 は検証しない。労働力状態不詳の分だけ
-- 15歳以上人口が多く、実測で最大10万人ずれる。
--
-- 値は万人単位で丸められているので、内訳の合計は数万人ずれうる（実測で最大1万人）。
--
-- 突き合わせだけだと片方が丸ごと消えたときに 0 行で成功してしまうので、
-- 比較できた組が1つも無い場合も落とす。

WITH pivoted AS (
    SELECT
        frequency,
        sex_code,
        age_class_code,
        area,
        time,
        MAX(value) FILTER (WHERE labor_status_code = '01') AS labor_force,
        MAX(value) FILTER (WHERE labor_status_code = '02') AS employed,
        MAX(value) FILTER (WHERE labor_status_code = '08') AS unemployed,
        MAX(value) FILTER (WHERE labor_status_code = '03') AS at_work,
        MAX(value) FILTER (WHERE labor_status_code = '07') AS on_leave
    FROM {{ ref('labor_force') }}
    WHERE industry_code = '000'
    GROUP BY frequency, sex_code, age_class_code, area, time
)

SELECT
    'labor_force_mismatch' AS check_name,
    frequency, area, time,
    labor_force - (employed + unemployed) AS difference
FROM pivoted
WHERE labor_force IS NOT NULL AND employed IS NOT NULL AND unemployed IS NOT NULL
  AND ABS(labor_force - (employed + unemployed)) > 2

UNION ALL

SELECT
    'employed_mismatch' AS check_name,
    frequency, area, time,
    employed - (at_work + on_leave) AS difference
FROM pivoted
WHERE employed IS NOT NULL AND at_work IS NOT NULL AND on_leave IS NOT NULL
  AND ABS(employed - (at_work + on_leave)) > 2

UNION ALL

SELECT
    'no_comparable_rows' AS check_name,
    NULL AS frequency, NULL AS area, NULL AS time, NULL AS difference
FROM pivoted
HAVING COUNT(*) FILTER (
    WHERE labor_force IS NOT NULL AND employed IS NOT NULL AND unemployed IS NOT NULL
) = 0
