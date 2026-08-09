{# SSDS の22表と、その元になる stg モデルの対応。

   表の名前は「分野1文字 + 地域粒度 + 分野名」で機械的に決まるので、組み合わせを
   書き下すのではなく組み立てる。22表を列挙する場所が増えるほど、表を1つ足したときに
   直し忘れる場所が増える。 #}
{% macro e_stat_ssds_marts() %}
    {% set categories = [
        ('a', 'population'), ('b', 'land'), ('c', 'economy'),
        ('d', 'administration'), ('e', 'education'), ('f', 'labor'),
        ('g', 'culture'), ('h', 'housing'), ('i', 'health'),
        ('j', 'welfare'), ('k', 'safety')
    ] %}
    {% set pairs = [] %}
    {% for level in ['pref', 'municipal'] %}
        {% for letter, domain in categories %}
            {% do pairs.append(
                (letter ~ '_' ~ level ~ '_' ~ domain, 'stg_' ~ level ~ '_' ~ domain)
            ) %}
        {% endfor %}
    {% endfor %}
    {{ return(pairs) }}
{% endmacro %}
