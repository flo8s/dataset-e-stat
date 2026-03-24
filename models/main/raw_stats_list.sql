SELECT *
FROM {{ source('estat_source', 'stats_list') }}
