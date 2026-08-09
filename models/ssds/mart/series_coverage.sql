{# 指標ごとの収録年。

   22表の year は指標によって終わる年が違う。a_pref_population はテーブル全体では
   2025年まで入っているが、2025年に届く cat01 は594指標中12指標だけで、307指標は
   国勢調査ベースなので2020年で止まる。テーブル単位の MAX(year) を「このデータは
   どこまで新しいか」として使うと、更新されていない指標まで更新済みとして扱われる。

   value が NULL の行（元データが「-」「X」）は数えない。欲しいのは「値がある最新の年」で、
   行があるだけの年ではない。

   year が NULL の行も数えない。year は time_name からの西暦4桁の抽出なので、和暦表記の
   系列が e-Stat 側に増えると NULL になる。落とさずに集計すると min_year も max_year も
   NULL の行がここに載り、収録年の整合性テストが dbt build ごと止める。収録年が読めない
   系列は行を持たない、が正しい答えで、それは e_stat_stg_transform の側で直す話になる。

   min_year / max_year / year_count は area をまたいだ和である（GROUP BY は cat01 だけ）。
   ある指標の max_year は「どこかの地域で」その年まで入っていることを表す。地域を1つに
   絞って描くときは、その地域にその年の行があるかを別に確かめること。c_municipal_economy
   では199系列中114系列で最新年が地域ごとに違う。

   item_name の MIN は cat01 との対応が1対1なので、値を選んでいるのではなく1つしかない
   値を取り出している。ビルドのたびに同じ結果になるよう ANY_VALUE は使わない。

   view ではなく table にしているのは、22表 1,900万行の集計を参照のたびに走らせないため。
   結果は5千行程度で、絞り込みは常にここへの1クエリで済む。 #}
{{ config(materialized='table') }}

{% set marts = e_stat_ssds_marts() %}

WITH coverage AS (
{% for mart, stg in marts %}
    SELECT
        '{{ mart }}' AS table_name,
        cat01,
        MIN(item_name) AS item_name,
        MIN(year) AS min_year,
        MAX(year) AS max_year,
        COUNT(DISTINCT year) AS year_count
    FROM {{ ref(mart) }}
    WHERE value IS NOT NULL AND year IS NOT NULL
    GROUP BY cat01
    {% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
)

SELECT * FROM coverage
ORDER BY table_name, cat01
