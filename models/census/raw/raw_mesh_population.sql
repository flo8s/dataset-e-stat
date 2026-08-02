{# 令和2年国勢調査 1kmメッシュ「人口及び世帯」。
   分類は cat01=年齢別人口・世帯の種類別世帯数等、cat02=秘匿・合算区分、area=メッシュコード。
   area は境界データ mesh_1km の mesh_code と同一体系(8桁)。 #}
SELECT
    cat01, cat02, area, unit, value,
    cat01_metadata, cat02_metadata, area_metadata
FROM {{ source('estat_source', 'mesh_population') }}
