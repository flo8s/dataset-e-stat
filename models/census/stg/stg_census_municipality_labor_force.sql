{# 令和2年国勢調査 就業状態等基本集計 第1-2-1表
   「男女，年齢（5歳階級），労働力状態別人口（15歳以上）」。
   分類は cat01=男女、cat02=年齢、cat03=労働力状態。

   cat02 は取得時に cdCat02="00"(年齢総数)で絞っているが、絞り込みが効かなかった
   ときに年齢階級の行が mart に混ざり (area, cat01, cat03) が一意でなくなるため、
   ここでも総数だけに限る。

   cat03 は総数・労働力人口・就業者・完全失業者…と階層になっていて、level が
   1(総数と大区分)/2(その内訳)/3(さらに内訳)を表す。level を無視して合計すると
   何重にも数える。 #}
SELECT
    area,
    area_metadata->>'$.name' AS area_name,
    {{ e_stat_municipality_area_level('area_metadata') }} AS area_level,
    area_metadata->>'$.parent_code' AS parent_area,
    cat01,
    cat01_metadata->>'$.name' AS sex,
    cat03,
    cat03_metadata->>'$.name' AS labor_status,
    TRY_CAST(cat03_metadata->>'$.level' AS INTEGER) AS labor_status_level,
    unit,
    TRY_CAST(value AS BIGINT) AS value
FROM {{ ref('raw_census_municipality_labor_force') }}
WHERE cat02 = '00'
