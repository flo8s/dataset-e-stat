-- SSDS の mart が stg の行数をそのまま保っていることを検証する。
-- 結果が0行ならテスト成功。行が返る場合、mart のビューに行を複製する結合が
-- 混入している（item_name / unit の参照表を item_code で一意化せずに JOIN した等）。
--
-- 元データ側は 22 テーブルすべてが tab 1種類・(cat01, area, time) 一意なので、
-- mart で行が増えるのは必ず変換側の欠陥になる。COUNT(*) の比較だけなので
-- GROUP BY で重複キーを探すより安く、掛け算が起きればどのみち行数がずれる。

{% set ssds_marts = e_stat_ssds_marts() %}

{% for mart, stg in ssds_marts %}
SELECT
    '{{ mart }}' AS model_name,
    (SELECT COUNT(*) FROM {{ ref(mart) }}) AS mart_rows,
    (SELECT COUNT(*) FROM {{ ref(stg) }}) AS stg_rows
WHERE (SELECT COUNT(*) FROM {{ ref(mart) }}) <> (SELECT COUNT(*) FROM {{ ref(stg) }})
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
