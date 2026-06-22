{# CPI は tab に「指数」「前月比・前年比・前年度比」「前年同月比」の 3 表章があり、
   いずれも分析上重要なため tab_name を保持する（汎用 e_stat_stg_transform は tab_name を落とす）。
   year は time_metadata の名称から西暦4桁を抽出している。 #}
SELECT
    tab,
    tab_metadata->>'$.name' AS tab_name,
    cat01,
    cat01_metadata->>'$.name' AS item_name,
    area,
    area_metadata->>'$.name' AS area_name,
    time,
    time_metadata->>'$.name' AS time_name,
    TRY_CAST(regexp_extract(time_metadata->>'$.name', '(\d{4})', 1) AS INTEGER) AS year,
    unit,
    TRY_CAST(value AS DOUBLE) AS value
FROM {{ ref('raw_cpi') }}
