-- 地域ブロックの合計が全国と一致することを検証する。結果が0行ならテスト成功。
--
-- 地域ブロックは全国を覆う区分で、同じ列に全国の行も入っている。九州は2010年代前半までが
-- 九州・沖縄地方（00055）、以降が九州地方（00057）と沖縄地方（00059）に分かれる。
-- e-Stat 側の切り替えがずれて同じ時点に両方が並ぶと、地域を足し上げた集計だけが
-- 静かに約2倍になる。行数も列も変わらないので、合計を全国と突き合わせて落とす。
--
-- 値は万人単位で丸められているので、10ブロックの合計は全国と数万人ずれうる
-- （実測で最大2万人）。
--
-- ブロックの一部が欠けている時点は対象外にする。欠けたまま比較すると丸めではない
-- 差が出て、切り替えの検知と区別が付かない。

WITH quarterly AS (
    SELECT sex_code, age_class_code, labor_status_code, area, time, value
    FROM {{ ref('labor_force') }}
    WHERE frequency = '四半期' AND industry_code = '000'
),
area_counts AS (
    SELECT time, COUNT(DISTINCT area) AS n_areas
    FROM quarterly
    WHERE area <> '00000'
    GROUP BY time
),
blocks AS (
    SELECT
        sex_code, age_class_code, labor_status_code, time,
        SUM(value) AS block_total,
        COUNT(value) AS n_values
    FROM quarterly
    WHERE area <> '00000'
    GROUP BY sex_code, age_class_code, labor_status_code, time
),
national AS (
    SELECT sex_code, age_class_code, labor_status_code, time, value AS national_total
    FROM quarterly
    WHERE area = '00000' AND value IS NOT NULL
),
compared AS (
    SELECT
        n.time, n.labor_status_code, n.sex_code, n.age_class_code,
        n.national_total, b.block_total
    FROM national n
    JOIN blocks b
      ON n.sex_code = b.sex_code
     AND n.age_class_code = b.age_class_code
     AND n.labor_status_code = b.labor_status_code
     AND n.time = b.time
    JOIN area_counts a ON a.time = n.time AND a.n_areas = b.n_values
)

SELECT
    'block_sum_mismatch' AS check_name,
    time, labor_status_code, sex_code, age_class_code,
    national_total - block_total AS difference
FROM compared
WHERE ABS(national_total - block_total) > 10

UNION ALL

SELECT
    'no_comparable_rows' AS check_name,
    NULL AS time, NULL AS labor_status_code, NULL AS sex_code,
    NULL AS age_class_code, NULL AS difference
FROM compared
HAVING COUNT(*) = 0
