{# census の area は市区町村(5桁)・町丁・字等(9桁)・その内訳(11桁)が同じ列に縦に
   並んでいて、桁数を数えないと粒度が分からない。5桁は9桁の合計、9桁は11桁の合計に
   あたるため、粒度を揃えずに合計すると二重・三重の計上になる。

   境界データ boundary.small_area の key_code は9桁と11桁のみで、5桁の市区町村行に
   対応する境界は無い。地図に載せるなら small_area / small_area_detail で絞る。 #}
{% macro e_stat_area_level(column) %}
CASE LENGTH({{ column }})
    WHEN 5 THEN 'municipality'
    WHEN 9 THEN 'small_area'
    WHEN 11 THEN 'small_area_detail'
END
{% endmacro %}

{# 市区町村・都道府県別の集計では area が全て5桁で、粒度は桁数から分からない。
   e-Stat のメタ情報が持つ level と parentCode で決まる:
     1 全国 / 2 都道府県 / 4 市・特別区部・特別区 / 5 政令指定都市の区 /
     6 町村 / 7 2000年(平成12年)市区町村
   level=4 には親が都道府県の「市・特別区部」と、親が特別区部(13100)の
   「特別区」の両方が入る。千代田区は level=4 で、政令指定都市の区(level=5)とは
   level が違うのに階層上は同じ位置にある。level だけで分けると特別区が市と
   同じ扱いになり、東京都の人口が二重に乗る。親コードまで見て分ける。 #}
{% macro e_stat_municipality_area_level(metadata_column) %}
CASE {{ metadata_column }}->>'$.level'
    WHEN '1' THEN 'national'
    WHEN '2' THEN 'prefecture'
    WHEN '4' THEN CASE
        WHEN {{ metadata_column }}->>'$.parent_code' LIKE '%000' THEN 'city'
        ELSE 'ward'
    END
    WHEN '5' THEN 'ward'
    WHEN '6' THEN 'town_village'
    WHEN '7' THEN 'former_municipality'
END
{% endmacro %}
