SELECT
    mesh_code,
    mesh1_code,
    mesh2_code,
    mesh3_code,
    geometry
FROM {{ ref('stg_mesh_boundary') }}
