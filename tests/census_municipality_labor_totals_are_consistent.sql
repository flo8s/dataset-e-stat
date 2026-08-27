-- 就業状態等基本集計の 2 表で、総数と内訳の恒等式が成り立つことを検証する。
-- 結果が0行ならテスト成功。
--
-- どちらの表も総数・大区分・その内訳が同じ列に縦に並ぶ縦持ちで、階層は
-- labor_status_level / employment_status_level と is_reprint にしか出ていない。
-- 原典のコードが振り直されたり階層の付け方が変わったりすると、行数も area_level も
-- 変わらないまま「合計に使える行」の集合だけがずれる。恒等式で落とす。
--
-- 恒等式は両辺を COALESCE(…, 0) で突き合わせる。片側だけ 0 に寄せると、
-- 内訳のコードが丸ごと変わって SUM が 0 行 = NULL を返したときに比較そのものが
-- NULL になり、テストが黙って通ってしまう。
-- 区分の顔ぶれが変わる壊れ方は恒等式では捉えられない（総数も内訳も揃って
-- 別のコードに移れば、両辺とも 0 になって辻褄が合う）ので、区分のコードが
-- 原典どおり揃っていることも併せて見る。
--
-- 原典が「-」の区分は NULL で入っており、該当者がいないこと(0人)を表すので
-- 0 とみなして足す。全町避難が続いた双葉町は全行 NULL で、両辺とも 0 になる。

{% set labor_status_codes = ['0', '1', '11', '111', '112', '113', '114', '12', '2', '21', '22', '23', '3'] %}
{% set employment_status_codes = ['0', '1', '11', '12', '13', '2', '3', '4', '5', '6', '7', 'R1'] %}

WITH labor_force AS (
    SELECT
        area,
        sex_code,
        LIST_SORT(ARRAY_AGG(DISTINCT labor_status_code)) AS category_codes,
        COALESCE(MAX(value) FILTER (WHERE labor_status_code = '0'), 0) AS total,
        COALESCE(SUM(value) FILTER (
            WHERE labor_status_level = 1 AND labor_status_code <> '0'
        ), 0) AS top_level_sum,
        COALESCE(MAX(value) FILTER (WHERE labor_status_code = '1'), 0) AS labor_force,
        COALESCE(SUM(value) FILTER (
            WHERE labor_status_code IN ('11', '12')
        ), 0) AS labor_force_sum,
        COALESCE(MAX(value) FILTER (WHERE labor_status_code = '11'), 0) AS employed,
        COALESCE(SUM(value) FILTER (
            WHERE labor_status_code IN ('111', '112', '113', '114')
        ), 0) AS employed_sum,
        COALESCE(MAX(value) FILTER (WHERE labor_status_code = '2'), 0) AS not_in_labor_force,
        COALESCE(SUM(value) FILTER (
            WHERE labor_status_code IN ('21', '22', '23')
        ), 0) AS not_in_labor_force_sum
    FROM {{ ref('census_municipality_labor_force') }}
    GROUP BY area, sex_code
),

employment_status AS (
    SELECT
        area,
        sex_code,
        LIST_SORT(ARRAY_AGG(DISTINCT employment_status_code)) AS category_codes,
        COALESCE(MAX(value) FILTER (WHERE employment_status_code = '0'), 0) AS total,
        COALESCE(SUM(value) FILTER (
            WHERE employment_status_level = 1
                AND employment_status_code <> '0'
                AND NOT is_reprint
        ), 0) AS top_level_sum,
        COALESCE(MAX(value) FILTER (WHERE employment_status_code = '1'), 0) AS employees,
        COALESCE(SUM(value) FILTER (
            WHERE employment_status_level = 2
        ), 0) AS employees_sum,
        COALESCE(MAX(value) FILTER (WHERE employment_status_code = 'R1'), 0) AS reprint,
        COALESCE(SUM(value) FILTER (
            WHERE employment_status_code IN ('1', '2')
        ), 0) AS reprint_sum
    FROM {{ ref('census_municipality_employment_status') }}
    GROUP BY area, sex_code
),

-- 男 + 女 = 総数。男女の区分が入れ替わっても行数は変わらない。
labor_force_by_sex AS (
    SELECT
        area,
        labor_status_code AS category_code,
        COALESCE(MAX(value) FILTER (WHERE sex_code = '0'), 0) AS both_sexes,
        COALESCE(SUM(value) FILTER (WHERE sex_code IN ('1', '2')), 0) AS male_female
    FROM {{ ref('census_municipality_labor_force') }}
    GROUP BY area, labor_status_code
),

employment_status_by_sex AS (
    SELECT
        area,
        employment_status_code AS category_code,
        COALESCE(MAX(value) FILTER (WHERE sex_code = '0'), 0) AS both_sexes,
        COALESCE(SUM(value) FILTER (WHERE sex_code IN ('1', '2')), 0) AS male_female
    FROM {{ ref('census_municipality_employment_status') }}
    GROUP BY area, employment_status_code
)

SELECT 'labor_force: 労働力状態の区分が原典と違う' AS violation, area, ARRAY_TO_STRING(category_codes, ',') AS code
FROM labor_force
WHERE category_codes <> LIST_SORT({{ labor_status_codes }}::VARCHAR[])
UNION ALL
SELECT 'labor_force: 総数 <> 大区分の和', area, sex_code
FROM labor_force WHERE total <> top_level_sum
UNION ALL
SELECT 'labor_force: 労働力人口 <> 就業者 + 完全失業者', area, sex_code
FROM labor_force WHERE labor_force <> labor_force_sum
UNION ALL
SELECT 'labor_force: 就業者 <> 内訳4区分の和', area, sex_code
FROM labor_force WHERE employed <> employed_sum
UNION ALL
SELECT 'labor_force: 非労働力人口 <> 内訳3区分の和', area, sex_code
FROM labor_force WHERE not_in_labor_force <> not_in_labor_force_sum
UNION ALL
SELECT 'labor_force: 総数 <> 男 + 女', area, category_code
FROM labor_force_by_sex WHERE both_sexes <> male_female
UNION ALL
SELECT 'employment_status: 従業上の地位の区分が原典と違う', area, ARRAY_TO_STRING(category_codes, ',')
FROM employment_status
WHERE category_codes <> LIST_SORT({{ employment_status_codes }}::VARCHAR[])
UNION ALL
SELECT 'employment_status: 総数 <> 大区分の和', area, sex_code
FROM employment_status WHERE total <> top_level_sum
UNION ALL
SELECT 'employment_status: 雇用者 <> 内訳3区分の和', area, sex_code
FROM employment_status WHERE employees <> employees_sum
UNION ALL
SELECT 'employment_status: 再掲 <> 雇用者 + 役員', area, sex_code
FROM employment_status WHERE reprint <> reprint_sum
UNION ALL
SELECT 'employment_status: 総数 <> 男 + 女', area, category_code
FROM employment_status_by_sex WHERE both_sexes <> male_female
