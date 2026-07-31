{{ config(materialized='table') }}

{#
  mart のビューが item_name / unit / 階層を引くための、item_code で一意な参照表。

  item_catalog は「どの統計表にその項目が載っているか」を表すため、同じ item_code が
  掲載統計表の数だけ（最大3行）存在する。mart のビューがこれを直接 LEFT JOIN していた
  ため、観測値1行が最大3行に増えていた。SSDS の 22 マートのうち 4,934 の
  (テーブル, cat01) 組のうち 1,657 組がこの掛け算の影響を受けていた。

  item_code ごとに unit が食い違う例は無く、item_name が食い違うのは 5,377 件中 8 件
  （全角/半角括弧などの表記ゆれ）のみ。stats_data_id の小さい統計表の表記を採る。

  階層は item_catalog の level / parent_code が使えない（level は全て 1、parent_code
  は全て空）ため、コードの前方一致から組み立てる。A1101 の下に A110101 が来る形。
  ここで出す親子はあくまでコードの前方一致であって、内訳が親を分割している保証は無い。
  A 人口・世帯では内訳（男/女）の合計が親に一致するが、J 福祉・社会保障の J1102 の
  ように「うち」項目だけが並んでいて親に届かない系統もある。
#}

WITH codes AS (
    SELECT DISTINCT item_code FROM {{ ref('item_catalog') }}
),

-- 自分より短く、かつ自分の先頭に一致するコードが祖先。最長のものが親。
ancestry AS (
    SELECT
        c.item_code,
        COUNT(*) AS ancestor_count,
        MAX(LENGTH(a.item_code)) AS parent_length
    FROM codes c
    JOIN codes a
        ON LENGTH(a.item_code) < LENGTH(c.item_code)
        AND STARTS_WITH(c.item_code, a.item_code)
    GROUP BY c.item_code
),

names AS (
    SELECT
        item_code,
        MIN_BY(item_name, stats_data_id) AS item_name,
        MIN_BY(unit, stats_data_id) AS unit
    FROM {{ ref('item_catalog') }}
    GROUP BY item_code
)

SELECT
    n.item_code,
    n.item_name,
    n.unit,
    (COALESCE(a.ancestor_count, 0) + 1)::INTEGER AS level,
    SUBSTR(n.item_code, 1, a.parent_length) AS parent_code
FROM names n
LEFT JOIN ancestry a ON n.item_code = a.item_code
