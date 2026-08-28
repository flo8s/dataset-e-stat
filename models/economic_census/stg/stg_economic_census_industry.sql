{# 令和3年経済センサス‐活動調査 産業横断的集計 売上（収入）金額等 第2-1表
   「産業(大分類)、経営組織(3区分)別民営事業所数、従業者数、売上（収入）金額、
   1事業所当たり従業者数、1事業所当たり売上（収入）金額及び従業者1人当たり
   売上（収入）金額」。
   分類は tab=表章項目(6)、cat01=産業大分類(27)、cat02=経営組織(6)、area=地域(1,966)。

   tab は測定項目で、項目ごとに単位が違う(事業所 / 人 / 百万円 / 万円)。
   実数の項目も1事業所当たりの項目も同じ value 列に入るため、mart で項目ごとの
   列に開く。ここでは縦持ちのまま名称と階層だけを解決する。単位は列ごとに決まり
   mart では持たないので、tab_metadata と unit は raw から先へ運ばない。

   value は原典が「-」(該当数字なし)・「･･･」(調査していない)・「X」(秘匿) を
   返すため TRY_CAST で NULL に落とす。3つの意味は区別できない。 #}
SELECT
    area,
    area_metadata->>'$.name' AS area_name,
    {{ e_stat_economic_census_area_level('area', 'area_metadata') }} AS area_level,
    area_metadata->>'$.parent_code' AS parent_area,
    cat01,
    cat01_metadata->>'$.name' AS industry_name,
    TRY_CAST(cat01_metadata->>'$.level' AS INTEGER) AS industry_level,
    cat01_metadata->>'$.parent_code' AS parent_industry,
    cat02,
    cat02_metadata->>'$.name' AS organization,
    tab,
    TRY_CAST(value AS DOUBLE) AS value
FROM {{ ref('raw_economic_census_industry') }}
