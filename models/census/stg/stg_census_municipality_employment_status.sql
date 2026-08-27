{# 令和2年国勢調査 就業状態等基本集計 第3-2表
   「男女，従業上の地位別就業者数（15歳以上）」。
   分類は cat01=男女、cat02=従業上の地位。

   cat02 は総数・雇用者・役員・業主…と階層になっていて、level が 1(総数と大区分)/
   2(雇用者の内訳)を表す。さらに「（再掲）雇用者（役員を含む）」は level=1 だが
   雇用者と役員の再掲なので、大区分だけを足しても二重に乗る。再掲はコードが R で
   始まるので、フラグに開いて合計から外せるようにする。 #}
SELECT
    area,
    area_metadata->>'$.name' AS area_name,
    {{ e_stat_municipality_area_level('area_metadata') }} AS area_level,
    area_metadata->>'$.parent_code' AS parent_area,
    cat01,
    cat01_metadata->>'$.name' AS sex,
    cat02,
    cat02_metadata->>'$.name' AS employment_status,
    TRY_CAST(cat02_metadata->>'$.level' AS INTEGER) AS employment_status_level,
    cat02 LIKE 'R%' AS is_reprint,
    unit,
    TRY_CAST(value AS BIGINT) AS value
FROM {{ ref('raw_census_municipality_employment_status') }}
