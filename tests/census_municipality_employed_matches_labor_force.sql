-- 従業上の地位別就業者数の総数が、労働力状態別人口の就業者と一致することを検証する。
-- 結果が0行ならテスト成功。
--
-- 2 表は原典の別々の統計表(第3-2表・第1-2-1表)から取っていて、統計表 ID は
-- 表番号と集計の名前で引いている。片方だけ別の表に差し替わっても、行数も
-- area_level も恒等式も表の中では閉じたまま通ってしまう。表をまたいで突き合わせる。
--
-- 内部結合なので、突き合わせる側のコードが振り直されると結合結果が 0 行になり、
-- 比較そのものが走らずに成功してしまう。突き合わせた行数が両表の該当行数と
-- 一致することも併せて見る。

WITH joined AS (
    SELECT
        e.area,
        e.sex_code,
        e.value AS employed_by_status,
        l.value AS employed_by_labor_force
    FROM {{ ref('census_municipality_employment_status') }} e
    JOIN {{ ref('census_municipality_labor_force') }} l
        ON e.area = l.area AND e.sex_code = l.sex_code
    WHERE e.employment_status_code = '0' AND l.labor_status_code = '11'
),

row_counts AS (
    SELECT
        (SELECT COUNT(*) FROM joined) AS joined_rows,
        (
            SELECT COUNT(*) FROM {{ ref('census_municipality_employment_status') }}
            WHERE employment_status_code = '0'
        ) AS employment_status_rows,
        (
            SELECT COUNT(*) FROM {{ ref('census_municipality_labor_force') }}
            WHERE labor_status_code = '11'
        ) AS labor_force_rows
)

SELECT '就業者数が一致しない' AS violation, area, sex_code, employed_by_status, employed_by_labor_force
FROM joined
WHERE employed_by_status IS DISTINCT FROM employed_by_labor_force
UNION ALL
SELECT '突き合わせた行数が両表の該当行数と一致しない', NULL, NULL, joined_rows, employment_status_rows
FROM row_counts
WHERE joined_rows <> employment_status_rows OR joined_rows <> labor_force_rows
