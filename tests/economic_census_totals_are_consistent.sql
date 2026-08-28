-- establishment_industry で、産業と経営組織の総数と内訳の恒等式が成り立つことを
-- 検証する。結果が0行ならテスト成功。
--
-- 産業も経営組織も総数・大分類・その内訳が同じ列に縦に並ぶ縦持ちで、階層は
-- industry_level / parent_industry と is_reprint にしか出ていない。原典のコードが
-- 振り直されたり階層の付け方が変わったりすると、行数も area_level も変わらないまま
-- 「合計に使える行」の集合だけがずれる。恒等式で落とす。
--
-- 恒等式は両辺を COALESCE(…, 0) で突き合わせる。片側だけ 0 に寄せると、内訳の
-- コードが丸ごと変わって SUM が 0 行 = NULL を返したときに比較そのものが NULL に
-- なり、テストが黙って通ってしまう。
-- 区分の顔ぶれが変わる壊れ方は恒等式では捉えられない(総数も内訳も揃って別の
-- コードに移れば両辺とも 0 になる)ので、区分のコードが原典どおり揃っていることも
-- 併せて見る。
--
-- 原典が「-」(該当数字なし)・「･･･」(調査していないもの)の区分は NULL で入っており、
-- 該当が無いこと(0)として足す。農林漁業の個人経営のように、総数には含まれるが
-- 区分としては調査されていない組み合わせがある。
--
-- 売上（収入）金額は恒等式に使えない。産業ごとに調査の有無が違ううえ、原典が区分
-- ごとに丸めているため内訳の和が総数と最大1百万円ずれる。事業所数と従業者数だけを見る。

{% set industry_codes = [
    'AR', 'AB', 'CR',
    'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R',
    'G1', 'G2', 'O1', 'O2', 'Q1', 'Q2', 'R1', 'R2'
] %}
{% set organization_codes = ['0', '1', '2', '3', 'S1', 'S2'] %}

WITH by_industry AS (
    -- 産業の恒等式は経営組織の総数の面で見る。
    SELECT
        area,
        LIST_SORT(ARRAY_AGG(DISTINCT industry_code)) AS category_codes,
        COALESCE(MAX(establishments) FILTER (WHERE industry_code = 'AR'), 0) AS total,
        COALESCE(SUM(establishments) FILTER (
            WHERE industry_code IN ('AB', 'CR')
        ), 0) AS top_level_sum,
        COALESCE(MAX(establishments) FILTER (WHERE industry_code = 'CR'), 0) AS non_primary,
        COALESCE(SUM(establishments) FILTER (
            WHERE industry_level = 2
        ), 0) AS major_sum,
        COALESCE(MAX(employees) FILTER (WHERE industry_code = 'AR'), 0) AS employee_total,
        COALESCE(SUM(employees) FILTER (
            WHERE industry_code IN ('AB', 'CR')
        ), 0) AS employee_top_level_sum
    FROM {{ ref('establishment_industry') }}
    WHERE organization_code = '0'
    GROUP BY area
),

-- 内訳を持つ大分類(G/O/Q/R)は、その内訳の和に一致する。
by_subdivision AS (
    SELECT
        p.area,
        p.industry_code,
        COALESCE(p.establishments, 0) AS parent_establishments,
        COALESCE(SUM(c.establishments), 0) AS child_establishments,
        COALESCE(p.employees, 0) AS parent_employees,
        COALESCE(SUM(c.employees), 0) AS child_employees
    FROM {{ ref('establishment_industry') }} p
    JOIN {{ ref('establishment_industry') }} c
        ON c.area = p.area
        AND c.organization_code = p.organization_code
        AND c.parent_industry = p.industry_code
    WHERE p.organization_code = '0' AND p.industry_level = 2
    GROUP BY p.area, p.industry_code, p.establishments, p.employees
),

by_organization AS (
    SELECT
        area,
        industry_code,
        LIST_SORT(ARRAY_AGG(DISTINCT organization_code)) AS category_codes,
        COALESCE(MAX(establishments) FILTER (WHERE organization_code = '0'), 0) AS total,
        COALESCE(SUM(establishments) FILTER (
            WHERE organization_code IN ('1', '2', '3')
        ), 0) AS breakdown_sum,
        COALESCE(MAX(employees) FILTER (WHERE organization_code = '0'), 0) AS employee_total,
        COALESCE(SUM(employees) FILTER (
            WHERE organization_code IN ('1', '2', '3')
        ), 0) AS employee_breakdown_sum
    FROM {{ ref('establishment_industry') }}
    GROUP BY area, industry_code
)

SELECT '産業の区分が原典と違う' AS violation, area, ARRAY_TO_STRING(category_codes, ',') AS detail
FROM by_industry
WHERE category_codes <> LIST_SORT({{ industry_codes }}::VARCHAR[])
UNION ALL
SELECT '全産業 <> 農林漁業 + 非農林漁業（事業所数）', area, NULL
FROM by_industry WHERE total <> top_level_sum
UNION ALL
SELECT '全産業 <> 農林漁業 + 非農林漁業（従業者数）', area, NULL
FROM by_industry WHERE employee_total <> employee_top_level_sum
UNION ALL
SELECT '非農林漁業 <> 大分類16区分の和（事業所数）', area, NULL
FROM by_industry WHERE non_primary <> major_sum
UNION ALL
SELECT '大分類 <> その内訳の和（事業所数）', area, industry_code
FROM by_subdivision WHERE parent_establishments <> child_establishments
UNION ALL
SELECT '大分類 <> その内訳の和（従業者数）', area, industry_code
FROM by_subdivision WHERE parent_employees <> child_employees
UNION ALL
SELECT '経営組織の区分が原典と違う', area, ARRAY_TO_STRING(category_codes, ',')
FROM by_organization
WHERE category_codes <> LIST_SORT({{ organization_codes }}::VARCHAR[])
UNION ALL
SELECT '総数 <> 個人 + 会社 + 会社以外の法人（事業所数）', area, industry_code
FROM by_organization WHERE total <> breakdown_sum
UNION ALL
SELECT '総数 <> 個人 + 会社 + 会社以外の法人（従業者数）', area, industry_code
FROM by_organization WHERE employee_total <> employee_breakdown_sum
