{# 家計調査の月次表と年次表を1つに積む。時間軸コードは月次が YYYY00MMMM、年次が
   YYYY000000 で衝突しないため、frequency で絞れば粒度が混ざらない。

   year / month は time_name の和暦揺れを避けて時間軸コードから切り出している
   （汎用 e_stat_stg_transform は time_name の正規表現から year を取る）。
   年次行の month は 00 になるので NULL に倒す。

   cat01 は総額・費目・品目が同じ列に混在するので、絞り込めるように e-Stat の
   メタ情報が持つ level と parentCode を item_level / item_parent として出す。

   UNION は BY NAME で積む。積む2表の列はすべて VARCHAR なので、列順だけが入れ替わっても
   型エラーにならず値が入れ替わったまま通ってしまう。 #}
{% macro e_stat_household_stg(monthly_model, annual_model) %}
WITH unioned AS (
    SELECT '月次' AS frequency, * FROM {{ ref(monthly_model) }}
    UNION ALL BY NAME
    SELECT '年次' AS frequency, * FROM {{ ref(annual_model) }}
)
SELECT
    cat01,
    cat01_metadata->>'$.name' AS item_name,
    TRY_CAST(cat01_metadata->>'$.level' AS INTEGER) AS item_level,
    cat01_metadata->>'$.parent_code' AS item_parent,
    cat02,
    cat02_metadata->>'$.name' AS household_type,
    area,
    area_metadata->>'$.name' AS area_name,
    time,
    time_metadata->>'$.name' AS time_name,
    frequency,
    TRY_CAST(substr(time, 1, 4) AS INTEGER) AS year,
    NULLIF(TRY_CAST(substr(time, 7, 2) AS INTEGER), 0) AS month,
    unit,
    TRY_CAST(value AS DOUBLE) AS value
FROM unioned
{% endmacro %}
