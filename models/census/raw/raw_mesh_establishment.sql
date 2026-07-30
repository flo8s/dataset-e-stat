{# 令和3年経済センサス‐活動調査 1kmメッシュ 産業別事業所数・従業者数
   (statsId T001157)。事業所の所在地=従業地ベースなので、昼間人口の流入側に使う。
   列は前半が事業所数、後半が同じ産業順の従業者数。
     T001157001 事業所数 A～S全産業
     T001157022 従業者数 A～S全産業
     T001157024 従業者数 C～E第2次産業
     T001157028 従業者数 F～S第3次産業 #}
SELECT
    KEY_CODE AS mesh_code,
    -- 配布は都道府県別で、県境をまたぐメッシュは各県のファイルに
    -- その県の分だけが入る。集約は stg で行う。
    REGEXP_EXTRACT(filename, '_(\d{2})\.csv$', 1) AS pref_code,
    T001157001 AS establishments,
    T001157022 AS employees_total,
    T001157024 AS employees_secondary,
    T001157028 AS employees_tertiary
FROM read_csv(
    'data/mesh_stats/T001157_*.csv',
    header = true,
    all_varchar = true,
    filename = true
)
