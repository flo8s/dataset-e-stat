{# dbt-duckdb のカタログマクロをオーバーライド。
   デフォルト DB のみにフィルタする問題を修正し、全アタッチ DB を対象にする。
   本体が修正されたらこのファイルを削除する。
   ref: https://github.com/duckdb/dbt-duckdb/blob/master/dbt/include/duckdb/macros/catalog.sql #}

{% macro duckdb__get_catalog(information_schema, schemas) -%}
  {%- call statement('catalog', fetch_result=True) -%}
    with relations AS (
      select
        t.table_name
        , t.database_name
        , t.schema_name
        , 'BASE TABLE' as table_type
        , t.comment as table_comment
      from duckdb_tables() t
      WHERE t.database_name NOT LIKE '__ducklake_metadata%'
        AND t.database_name NOT IN ('system', 'temp', 'memory')
      UNION ALL
      SELECT v.view_name as table_name
      , v.database_name
      , v.schema_name
      , 'VIEW' as table_type
      , v.comment as table_comment
      from duckdb_views() v
      WHERE v.database_name NOT LIKE '__ducklake_metadata%'
        AND v.database_name NOT IN ('system', 'temp', 'memory')
    )
    select
        r.database_name as table_database,
        r.schema_name as table_schema,
        r.table_name,
        r.table_type,
        r.table_comment,
        c.column_name,
        c.column_index as column_index,
        c.data_type as column_type,
        c.comment as column_comment,
        NULL as table_owner
    FROM relations r
    JOIN duckdb_columns() c
      ON r.database_name = c.database_name
      AND r.schema_name = c.schema_name
      AND r.table_name = c.table_name
    WHERE (
        {%- for schema in schemas -%}
          upper(r.schema_name) = upper('{{ schema }}'){%- if not loop.last %} or {% endif -%}
        {%- endfor -%}
    )
    ORDER BY
        r.database_name,
        r.schema_name,
        r.table_name,
        c.column_index
  {%- endcall -%}
  {{ return(load_result('catalog').table) }}
{%- endmacro %}
