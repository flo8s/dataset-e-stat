{# 労働力調査 基本集計の全国月次表と地域別四半期表を1つに積む。

   2つの表は分類軸の並びが違う。全国は cat01=産業・cat02=性別・cat03=就業状態・
   cat04=年齢階級、地域別は産業の軸を持たず cat01=性別・cat02=年齢階級・cat03=就業状態。
   コード体系は共通で、地域別の年齢階級は全国の部分集合（18区分 ⊂ 30区分）。

   地域別の値は産業を分けない集計なので、産業コードには全国表の「全産業」と同じ
   000 を入れている。これで industry_code = '000' が粒度によらず総数になる。

   時間軸コードは月次が YYYY00MMMM（2024000404 = 2024年4月）、四半期が
   YYYY00MMMM（2024000406 = 2024年4～6月期）で、期末月の位置が違うだけで衝突しない。
   月次行の quarter と四半期行の month は NULL に倒す。

   UNION は列を明示して積む。BY NAME に頼ると、片方の表に e-Stat が軸を足したときに
   列が入れ替わったまま通ってしまう。 #}
{% macro e_stat_labor_force_stg(monthly_model, regional_model) %}
SELECT
    '月次' AS frequency,
    cat01 AS industry_code,
    cat01_metadata->>'$.name' AS industry_name,
    cat02 AS sex_code,
    cat02_metadata->>'$.name' AS sex,
    cat03 AS labor_status_code,
    cat03_metadata->>'$.name' AS labor_status,
    TRY_CAST(cat03_metadata->>'$.level' AS INTEGER) AS labor_status_level,
    cat04 AS age_class_code,
    cat04_metadata->>'$.name' AS age_class,
    TRY_CAST(cat04_metadata->>'$.level' AS INTEGER) AS age_class_level,
    cat04_metadata->>'$.parent_code' AS age_class_parent,
    area,
    area_metadata->>'$.name' AS area_name,
    time,
    time_metadata->>'$.name' AS time_name,
    TRY_CAST(substr(time, 1, 4) AS INTEGER) AS year,
    TRY_CAST(substr(time, 7, 2) AS INTEGER) AS month,
    CAST(NULL AS INTEGER) AS quarter,
    unit,
    TRY_CAST(value AS DOUBLE) AS value
FROM {{ ref(monthly_model) }}

UNION ALL

SELECT
    '四半期' AS frequency,
    '000' AS industry_code,
    '全産業' AS industry_name,
    cat01 AS sex_code,
    cat01_metadata->>'$.name' AS sex,
    cat03 AS labor_status_code,
    cat03_metadata->>'$.name' AS labor_status,
    TRY_CAST(cat03_metadata->>'$.level' AS INTEGER) AS labor_status_level,
    cat02 AS age_class_code,
    cat02_metadata->>'$.name' AS age_class,
    TRY_CAST(cat02_metadata->>'$.level' AS INTEGER) AS age_class_level,
    cat02_metadata->>'$.parent_code' AS age_class_parent,
    area,
    area_metadata->>'$.name' AS area_name,
    time,
    time_metadata->>'$.name' AS time_name,
    TRY_CAST(substr(time, 1, 4) AS INTEGER) AS year,
    CAST(NULL AS INTEGER) AS month,
    TRY_CAST(substr(time, 9, 2) AS INTEGER) // 3 AS quarter,
    unit,
    TRY_CAST(value AS DOUBLE) AS value
FROM {{ ref(regional_model) }}
{% endmacro %}
