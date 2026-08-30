{# 率の表は就業状態（cat03）の名称が「労働力人口」「就業」「完全失業者」で、
   人数の表と同じ名前のまま比率を指している。そのまま出すと実数と見分けが付かないので、
   指標名を明示した indicator に置き換える。

   マッピングに無いコードが来たら indicator が NULL になる。e-Stat が指標を足しても
   行数も値も変わらないまま黙って通るので、labor_force_rate_indicator_is_exhaustive
   で落としている。 #}
SELECT
    frequency,
    sex_code,
    sex,
    labor_status_code AS indicator_code,
    CASE labor_status_code
        WHEN '01' THEN '労働力人口比率'
        WHEN '13' THEN '就業率'
        WHEN '08' THEN '完全失業率'
    END AS indicator,
    age_class_code,
    age_class,
    age_class_level,
    age_class_parent,
    area,
    area_name,
    time,
    time_name,
    year,
    month,
    quarter,
    unit,
    value
FROM (
    {{ e_stat_labor_force_stg('raw_labor_force_rate_monthly', 'raw_labor_force_rate_regional_quarterly') }}
)
