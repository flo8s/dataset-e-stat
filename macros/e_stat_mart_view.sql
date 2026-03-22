{% macro e_stat_mart_view(source_ref) %}
SELECT
    s.cat01,
    COALESCE(c.item_name, s.item_name) AS item_name,
    c.domain,
    s.area,
    s.area_name,
    s.time_name,
    s.year,
    COALESCE(c.unit, s.unit) AS unit,
    s.value
FROM {{ ref(source_ref) }} s
LEFT JOIN {{ ref('item_catalog') }} c ON s.cat01 = c.item_code
{% endmacro %}
