{{ config(materialized='table') }}

{#
  mart のビューが item_name / unit を引くための、item_code で一意な参照表。

  item_catalog は「どの統計表にその項目が載っているか」を表すため、同じ item_code が
  掲載統計表の数だけ（最大3行）存在する。mart のビューがこれを直接 LEFT JOIN していた
  ため、観測値1行が最大3行に増えていた。SSDS の 22 マートのうち 4,934 の
  (テーブル, cat01) 組のうち 1,657 組がこの掛け算の影響を受けていた。

  item_code ごとに unit が食い違う例は無く、item_name が食い違うのは 5,377 件中 8 件
  （全角/半角括弧などの表記ゆれ）のみ。stats_data_id の小さい統計表の表記を採る。
#}

SELECT
    item_code,
    MIN_BY(item_name, stats_data_id) AS item_name,
    MIN_BY(unit, stats_data_id) AS unit
FROM {{ ref('item_catalog') }}
GROUP BY item_code
