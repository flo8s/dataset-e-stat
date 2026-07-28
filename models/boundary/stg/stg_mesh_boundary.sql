SELECT
    KEY_CODE AS mesh_code,
    MESH1_ID AS mesh1_code,
    MESH2_ID AS mesh2_code,
    MESH3_ID AS mesh3_code,
    geom AS geometry
FROM {{ ref('raw_mesh_boundary') }}
WHERE geom IS NOT NULL
