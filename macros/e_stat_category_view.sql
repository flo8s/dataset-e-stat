{% macro e_stat_category_view(source_ref) %}
SELECT
    cat01,
    cat01_metadata->>'$.name' AS item_name,
    area,
    area_metadata->>'$.name' AS area_name,
    time,
    time_metadata->>'$.name' AS time_name,
    unit,
    value
FROM {{ source_ref }}
{% endmacro %}
