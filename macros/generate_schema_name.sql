{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if custom_schema_name is not none -%}
    {{ custom_schema_name | trim }}
  {%- elif node.fqn | length > 2 -%}
    {{ node.fqn[1] }}
  {%- else -%}
    {{ target.schema }}
  {%- endif -%}
{%- endmacro %}
