{# item_name / unit は item_code で一意化した stg_item_lookup から引く。
   item_catalog を直接 JOIN すると、同じ item_code が掲載統計表の数だけ存在するため
   観測値1行が最大3行に複製される。 #}
{% macro e_stat_mart_view(source_ref) %}
SELECT
    s.cat01,
    COALESCE(c.item_name, s.item_name) AS item_name,
    s.area,
    s.area_name,
    s.time_name,
    s.year,
    COALESCE(c.unit, s.unit) AS unit,
    s.value
FROM {{ ref(source_ref) }} s
LEFT JOIN {{ ref('stg_item_lookup') }} c ON s.cat01 = c.item_code
{% endmacro %}
