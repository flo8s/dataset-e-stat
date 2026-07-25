-- SSDS の mart が stg の行数をそのまま保っていることを検証する。
-- 結果が0行ならテスト成功。行が返る場合、mart のビューに行を複製する結合が
-- 混入している（item_name / unit の参照表を item_code で一意化せずに JOIN した等）。
--
-- 元データ側は 22 テーブルすべてが tab 1種類・(cat01, area, time) 一意なので、
-- mart で行が増えるのは必ず変換側の欠陥になる。COUNT(*) の比較だけなので
-- GROUP BY で重複キーを探すより安く、掛け算が起きればどのみち行数がずれる。

{% set ssds_marts = [
    ('a_pref_population', 'stg_pref_population'),
    ('b_pref_land', 'stg_pref_land'),
    ('c_pref_economy', 'stg_pref_economy'),
    ('d_pref_administration', 'stg_pref_administration'),
    ('e_pref_education', 'stg_pref_education'),
    ('f_pref_labor', 'stg_pref_labor'),
    ('g_pref_culture', 'stg_pref_culture'),
    ('h_pref_housing', 'stg_pref_housing'),
    ('i_pref_health', 'stg_pref_health'),
    ('j_pref_welfare', 'stg_pref_welfare'),
    ('k_pref_safety', 'stg_pref_safety'),
    ('a_municipal_population', 'stg_municipal_population'),
    ('b_municipal_land', 'stg_municipal_land'),
    ('c_municipal_economy', 'stg_municipal_economy'),
    ('d_municipal_administration', 'stg_municipal_administration'),
    ('e_municipal_education', 'stg_municipal_education'),
    ('f_municipal_labor', 'stg_municipal_labor'),
    ('g_municipal_culture', 'stg_municipal_culture'),
    ('h_municipal_housing', 'stg_municipal_housing'),
    ('i_municipal_health', 'stg_municipal_health'),
    ('j_municipal_welfare', 'stg_municipal_welfare'),
    ('k_municipal_safety', 'stg_municipal_safety')
] %}

{% for mart, stg in ssds_marts %}
SELECT
    '{{ mart }}' AS model_name,
    (SELECT COUNT(*) FROM {{ ref(mart) }}) AS mart_rows,
    (SELECT COUNT(*) FROM {{ ref(stg) }}) AS stg_rows
WHERE (SELECT COUNT(*) FROM {{ ref(mart) }}) <> (SELECT COUNT(*) FROM {{ ref(stg) }})
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
