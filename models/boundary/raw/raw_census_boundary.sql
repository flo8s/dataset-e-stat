{% set prefectures = range(1, 48) %}

{% for code in prefectures %}
{% if not loop.first %}UNION ALL{% endif %}
SELECT *
FROM ST_Read('data/census_boundary/r2ka{{ '%02d' | format(code) }}.gml')
{% endfor %}
