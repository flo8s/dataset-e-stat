{# 令和2年国勢調査 人口等基本集計 第1-1-1表「男女別人口」。
   分類は cat01=男女(総数/男/女) だけで、時間軸は 2020年 の 1 値しかない。

   area の粒度は桁数では分からない(全て5桁)。e-Stat のメタ情報が持つ level と
   parentCode で決まる:
     1 全国 / 2 都道府県 / 4 市・特別区部・特別区 / 5 政令指定都市の区 /
     6 町村 / 7 2000年(平成12年)市区町村
   level=4 には親が都道府県の「市・特別区部」と、親が特別区部(13100)の
   「特別区」の両方が入る。千代田区は level=4 で、政令指定都市の区(level=5)とは
   level が違うのに階層上は同じ位置にある。level だけで分けると特別区が市と
   同じ扱いになり、東京都の人口が二重に乗る。親コードまで見て分ける。 #}
SELECT
    area,
    area_metadata->>'$.name' AS area_name,
    CASE area_metadata->>'$.level'
        WHEN '1' THEN 'national'
        WHEN '2' THEN 'prefecture'
        WHEN '4' THEN CASE
            WHEN area_metadata->>'$.parent_code' LIKE '%000' THEN 'city'
            ELSE 'ward'
        END
        WHEN '5' THEN 'ward'
        WHEN '6' THEN 'town_village'
        WHEN '7' THEN 'former_municipality'
    END AS area_level,
    area_metadata->>'$.parent_code' AS parent_area,
    cat01,
    cat01_metadata->>'$.name' AS sex,
    TRY_CAST(value AS BIGINT) AS value
FROM {{ ref('raw_census_municipality_population') }}
