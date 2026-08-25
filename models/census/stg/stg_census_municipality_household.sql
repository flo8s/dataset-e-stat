{# 令和2年国勢調査 人口等基本集計 第1-1-2表
   「世帯の種類別世帯数及び世帯人員」。
   tab=表章事項(世帯数/世帯人員)、cat01=世帯の種類(総数/一般世帯/施設等の世帯)。
   単位が tab で変わる(世帯 / 人)ので、縦持ちのまま合計してはいけない。 #}
SELECT
    area,
    tab,
    tab_metadata->>'$.name' AS item_name,
    cat01,
    cat01_metadata->>'$.name' AS household_type,
    unit,
    TRY_CAST(value AS BIGINT) AS value
FROM {{ ref('raw_census_municipality_household') }}
