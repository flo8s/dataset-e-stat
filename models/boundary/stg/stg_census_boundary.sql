SELECT
    KEY_CODE AS key_code,
    PREF AS prefecture_code,
    PREF_NAME AS prefecture_name,
    CITY AS city_code,
    CITY_NAME AS city_name,
    S_NAME AS area_name,
    HCODE AS hcode,
    KIGO_E AS kigo_e,
    KIGO_D AS kigo_d,
    KIGO_I AS kigo_i,
    AREA_MAX_F AS area_max_f,
    JINKO AS jinko,
    SETAI AS setai,
    X_CODE AS x_code,
    Y_CODE AS y_code,
    AREA AS area_m2,
    geom AS geometry
FROM {{ ref('raw_census_boundary') }}
WHERE geom IS NOT NULL
