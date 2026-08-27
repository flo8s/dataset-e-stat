{# 令和2年国勢調査 人口等基本集計 第1-1-1表「男女別人口」。
   分類は cat01=男女(総数/男/女) だけで、時間軸は 2020年 の 1 値しかない。 #}
SELECT
    area,
    area_metadata->>'$.name' AS area_name,
    {{ e_stat_municipality_area_level('area_metadata') }} AS area_level,
    area_metadata->>'$.parent_code' AS parent_area,
    cat01,
    cat01_metadata->>'$.name' AS sex,
    TRY_CAST(value AS BIGINT) AS value
FROM {{ ref('raw_census_municipality_population') }}
