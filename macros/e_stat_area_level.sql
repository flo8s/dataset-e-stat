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
