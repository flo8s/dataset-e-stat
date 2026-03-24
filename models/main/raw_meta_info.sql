SELECT *
FROM {{ source('estat_source', 'meta_info') }}
