-- 消費支出が10大費目の合計と一致することを検証する。結果が0行ならテスト成功。
--
-- 品目階層は消費支出を根とする1本の木ではない。level=1 には消費支出・10大費目に加えて
-- 財・サービス支出計や消費支出(基礎・選択)のような別集計も並んでいる。どれが内訳で
-- どれが別集計かは level からは読めないので、恒等式が成り立つ組み合わせをここで固定する。
-- 品目分類が改定されて10大費目の構成が変わると、行数も level も変わらないまま
-- 「level=1 を足せば消費支出」という前提だけが崩れる。
--
-- 各月の値は円未満で丸められているので、合計は元の消費支出と数円ずれうる。

{% set major_groups = [
    '010000000', '020000000', '030000000', '040000000', '050000000',
    '060000000', '070000000', '080000000', '090000000', '100000000'
] %}

WITH scoped AS (
    SELECT cat02, area, time, cat01, value
    FROM {{ ref('expenditure') }}
    WHERE cat01 IN ('001100000'{% for g in major_groups %}, '{{ g }}'{% endfor %})
),
pivoted AS (
    SELECT
        cat02,
        area,
        time,
        MAX(value) FILTER (WHERE cat01 = '001100000') AS total,
        SUM(value) FILTER (WHERE cat01 <> '001100000') AS parts,
        COUNT(value) FILTER (WHERE cat01 <> '001100000') AS group_count
    FROM scoped
    GROUP BY cat02, area, time
)
SELECT cat02, area, time, total, parts, total - parts AS difference
FROM pivoted
WHERE total IS NOT NULL
  AND group_count = {{ major_groups | length }}
  AND ABS(total - parts) > 10
