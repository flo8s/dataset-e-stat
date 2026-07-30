{# 令和2年国勢調査 1kmメッシュ 労働力状態別人口・産業別就業者数 (statsId T001179)。
   いずれも常住地ベース。昼間人口の推計で使うのは次の4項目。
     T001179007 完全失業者 総数
     T001179010 非労働力人口 総数 (15歳以上)
     T001179013 第1次産業 総数 (常住地で従業する農林漁業就業者の代理)
     T001179004 就業者 総数 (検証用) #}
SELECT
    KEY_CODE AS mesh_code,
    -- 配布は都道府県別で、県境をまたぐメッシュは各県のファイルに
    -- その県の分だけが入る。集約は stg で行う。
    REGEXP_EXTRACT(filename, '_(\d{2})\.csv$', 1) AS pref_code,
    T001179004 AS employed_residents,
    T001179007 AS unemployed,
    T001179010 AS non_labor_force,
    T001179013 AS primary_industry_residents
FROM read_csv(
    'data/mesh_stats/T001179_*.csv',
    header = true,
    all_varchar = true,
    filename = true
)
