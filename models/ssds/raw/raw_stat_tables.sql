SELECT *
FROM {{ source('estat_source', 'stat_tables') }}
