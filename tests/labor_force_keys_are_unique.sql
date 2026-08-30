-- labor スキーマの mart が観測1つにつき1行であることを検証する。
-- 結果が0行ならテスト成功。
--
-- 2つの mart は統計表2つの UNION ALL で、時間軸コードが月次（2024000404）と
-- 四半期（2024000406）で衝突しないことを前提にしている。前提が崩れて同じ time が
-- 両方の粒度に現れると、frequency が違うので一意性の検査は素通りする一方、
-- time だけで並べた集計が静かに粒度を混ぜる。time の重なりも別に見る。
--
-- 率の表は e-Stat 側で産業の軸（全国表の cat01）が1値しか無いことに乗って、
-- その軸を落としている。産業の内訳が増えると (指標, 性別, 年齢階級, 地域, 時点) が
-- 一意でなくなり、指標名も行数の見た目も変わらないまま値が複製される。

SELECT
    'labor_force' AS model_name,
    frequency, industry_code, sex_code, labor_status_code, age_class_code, area, time,
    COUNT(*) AS n
FROM {{ ref('labor_force') }}
GROUP BY frequency, industry_code, sex_code, labor_status_code, age_class_code, area, time
HAVING COUNT(*) > 1

UNION ALL

SELECT
    'labor_force_rate' AS model_name,
    frequency, NULL AS industry_code, sex_code, indicator_code, age_class_code, area, time,
    COUNT(*) AS n
FROM {{ ref('labor_force_rate') }}
GROUP BY frequency, sex_code, indicator_code, age_class_code, area, time
HAVING COUNT(*) > 1

UNION ALL

SELECT
    'time_code_collision' AS model_name,
    NULL AS frequency, NULL AS industry_code, NULL AS sex_code,
    NULL AS labor_status_code, NULL AS age_class_code, NULL AS area, time,
    COUNT(DISTINCT frequency) AS n
FROM {{ ref('labor_force') }}
GROUP BY time
HAVING COUNT(DISTINCT frequency) > 1
