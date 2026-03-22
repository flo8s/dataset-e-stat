{# year は time_name から西暦4桁を正規表現抽出している。
   和暦表記（令和2年、平成17年等）には未対応。
   カレンダーデータセットによる和暦→西暦変換で別PR対応予定。 #}
{% macro e_stat_stg_transform(source_ref) %}
SELECT
    tab,
    cat01,
    cat01_metadata->>'$.name' AS item_name,
    area,
    area_metadata->>'$.name' AS area_name,
    time,
    time_metadata->>'$.name' AS time_name,
    TRY_CAST(regexp_extract(time_metadata->>'$.name', '(\d{4})', 1) AS INTEGER) AS year,
    unit,
    TRY_CAST(value AS DOUBLE) AS value
FROM {{ source_ref }}
{% endmacro %}
