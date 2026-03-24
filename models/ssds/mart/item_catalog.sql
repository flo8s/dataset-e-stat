SELECT *
FROM {{ ref('meta_info') }}
WHERE stats_data_id IN (
    SELECT DISTINCT aid FROM {{ ref('stats_list') }}
    -- 社会・人口統計体系: https://www.e-stat.go.jp/statistics/00200502
    WHERE stat_name__acode = '00200502'
)
AND class_id NOT IN ('tab', 'area', 'time')
