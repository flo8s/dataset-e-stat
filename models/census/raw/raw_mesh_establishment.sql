{# 平成28年経済センサス‐活動調査 1kmメッシュ「産業（大分類）別事業所数及び従業者数」。
   分類は cat01=産業大分類×事業所数/従業者数、area=メッシュコード。cat02 は無い。
   事業所の所在地=従業地ベースなので、昼間人口の流入側の配分ウェイトに使う。 #}
SELECT
    cat01, area, unit, value,
    cat01_metadata, area_metadata
FROM {{ source('estat_source', 'mesh_establishment') }}
