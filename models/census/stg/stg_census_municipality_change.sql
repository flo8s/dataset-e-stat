{# 令和2年国勢調査 人口等基本集計 第1-1-3表
   「2015年の人口・世帯数(組替)、5年間の増減、人口性比、面積、人口密度」。
   分類は tab=表章事項(9項目)だけで、項目ごとに単位が違う(人 / 世帯 / ％ /
   km2 / 1km2当たり)。人口性比だけ単位が無い。
   実数の項目も比率の項目も同じ value 列に入るため、mart で項目ごとの列に開く。 #}
SELECT
    area,
    tab,
    tab_metadata->>'$.name' AS item_name,
    unit,
    TRY_CAST(value AS DOUBLE) AS value
FROM {{ ref('raw_census_municipality_change') }}
