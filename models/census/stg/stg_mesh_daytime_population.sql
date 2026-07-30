{# 1kmメッシュ別の昼間人口推計。国勢調査(令和2年)と経済センサス(令和3年)の
   メッシュ統計を mesh_code で突き合わせる。

   ■ 推計式

     残留人口 = 夜間人口 − 当地に常住する就業者・通学者(15歳以上、自宅従業を含む)
     流入人口 = 経済センサス 第2次産業従業者数 + 第3次産業従業者数
                + 国勢調査 第1次産業就業者数(常住地)
     昼間人口 = 残留人口 + 流入人口

   自宅で従業する人はいったん残留人口から引かれ、事業所の従業者として流入人口で
   戻る。第1次産業は経済センサスの第2次・第3次に入らないので、職場が居住地の近傍
   であることを使って常住地ベースの値を足す。15歳未満は全員その場に残るものとして
   扱う (小中学校は学区内にあるため)。

   残留人口を「非労働力人口 + 未就学者 + 完全失業者」のように積み上げる形にはしない。
   令和2年国勢調査は労働力状態「不詳」が激増しており (東京都で約296万人)、
   労働力人口と非労働力人口の合計が15歳以上人口に届かないため、積み上げでは残留人口が
   1割以上不足する。上の式は不詳のない夜間人口から引くので影響を受けない。

   ■ 都道府県単位のキャリブレーション

     補正係数 = 公表昼間人口(都道府県) / 素推計の都道府県合計

   素推計には次の系統誤差が残る。
     - 経済センサスの従業者数は事業所への所属ベースで、出向・派遣や複数事業所勤務が
       重複する。東京都では国勢調査の従業地による就業者数 (SSDS F2801, 8,029,649) に
       対して 1,000万人を超える。
     - 従業地・通学地が「不詳」の常住者は流出側に含まれる。
   都道府県単位で公表昼間人口 (SSDS A6107) に一致するようスケールしてこれを吸収する。
   メッシュ間の分布は経済センサスと国勢調査の実測値のまま保たれる。未補正値も
   daytime_population_uncalibrated として残すので、補正の効き方は確認できる。

   ■ 秘匿値

   秘匿値 '*' は TRY_CAST で NULL になる。統計GIS のメッシュ統計は秘匿したメッシュの
   値を HTKSAKI が指す合算先メッシュへ寄せているため、NULL を 0 として扱えば
   都道府県合計・全国合計は保たれる (東京都・埼玉県・大阪府で実測: 人口総数の合計が
   公表値と完全に一致)。

   経済センサスにしか出てこないメッシュ (事業所だけがある工業地帯など) を落とさない
   よう FULL JOIN する。 #}

WITH population AS (
    SELECT
        mesh_code,
        pref_code,
        SUM(TRY_CAST(population_total AS BIGINT)) AS nighttime_population
    FROM {{ ref('raw_mesh_population') }}
    GROUP BY mesh_code, pref_code
),

labor AS (
    SELECT
        mesh_code,
        pref_code,
        SUM(TRY_CAST(employed_residents AS BIGINT)) AS employed_residents,
        SUM(TRY_CAST(unemployed AS BIGINT)) AS unemployed,
        SUM(TRY_CAST(non_labor_force AS BIGINT)) AS non_labor_force,
        SUM(TRY_CAST(primary_industry_residents AS BIGINT)) AS primary_industry_residents
    FROM {{ ref('raw_mesh_labor') }}
    GROUP BY mesh_code, pref_code
),

commute AS (
    SELECT
        mesh_code,
        pref_code,
        SUM(TRY_CAST(residents_working_or_studying AS BIGINT)) AS residents_working_or_studying,
        SUM(TRY_CAST(students_commuting_out AS BIGINT)) AS students_commuting_out,
        SUM(TRY_CAST(preschool_children AS BIGINT)) AS preschool_children,
        SUM(TRY_CAST(resident_students AS BIGINT)) AS resident_students,
        SUM(TRY_CAST(resident_students_compulsory AS BIGINT)) AS resident_students_compulsory
    FROM {{ ref('raw_mesh_commute') }}
    GROUP BY mesh_code, pref_code
),

establishment AS (
    SELECT
        mesh_code,
        pref_code,
        SUM(TRY_CAST(establishments AS BIGINT)) AS establishments,
        SUM(TRY_CAST(employees_total AS BIGINT)) AS employees_total,
        SUM(TRY_CAST(employees_secondary AS BIGINT)) AS employees_secondary,
        SUM(TRY_CAST(employees_tertiary AS BIGINT)) AS employees_tertiary
    FROM {{ ref('raw_mesh_establishment') }}
    GROUP BY mesh_code, pref_code
),

-- 県境をまたぐメッシュは県ごとに別行のまま置く。補正係数を県別に掛けてから
-- mesh_code で合算する必要があるため。
by_pref AS (
    SELECT
        COALESCE(p.mesh_code, l.mesh_code, c.mesh_code, e.mesh_code) AS mesh_code,
        COALESCE(p.pref_code, l.pref_code, c.pref_code, e.pref_code) AS pref_code,
        COALESCE(p.nighttime_population, 0) AS nighttime_population,
        COALESCE(l.non_labor_force, 0) AS non_labor_force,
        COALESCE(l.unemployed, 0) AS unemployed,
        COALESCE(l.employed_residents, 0) AS employed_residents,
        COALESCE(l.primary_industry_residents, 0) AS primary_industry_residents,
        COALESCE(c.residents_working_or_studying, 0) AS residents_working_or_studying,
        COALESCE(c.preschool_children, 0) AS preschool_children,
        COALESCE(c.resident_students, 0) AS resident_students,
        COALESCE(c.resident_students_compulsory, 0) AS resident_students_compulsory,
        COALESCE(c.students_commuting_out, 0) AS students_commuting_out,
        COALESCE(e.establishments, 0) AS establishments,
        COALESCE(e.employees_total, 0) AS employees_total,
        COALESCE(e.employees_secondary, 0) AS employees_secondary,
        COALESCE(e.employees_tertiary, 0) AS employees_tertiary
    FROM population AS p
    FULL JOIN labor AS l
        ON p.mesh_code = l.mesh_code AND p.pref_code = l.pref_code
    FULL JOIN commute AS c
        ON COALESCE(p.mesh_code, l.mesh_code) = c.mesh_code
            AND COALESCE(p.pref_code, l.pref_code) = c.pref_code
    FULL JOIN establishment AS e
        ON COALESCE(p.mesh_code, l.mesh_code, c.mesh_code) = e.mesh_code
            AND COALESCE(p.pref_code, l.pref_code, c.pref_code) = e.pref_code
),

raw_estimate AS (
    SELECT
        *,
        -- 残留人口は 0 で下限を切る。秘匿処理で夜間人口だけが合算先メッシュへ
        -- 寄せられると就業者・通学者が夜間人口を上回り、残留人口が負になる
        -- メッシュがわずかに出る (3都府県で6,412メッシュ中5件、最小 -13)。
        GREATEST(nighttime_population - residents_working_or_studying, 0)
        + primary_industry_residents
        + employees_secondary
        + employees_tertiary AS daytime_raw
    FROM by_pref
),

-- 公表昼間人口 (SSDS A6107、令和2年)。都道府県コードは area の上2桁。
-- area には全国 (00000) の行もあるので除く。
official AS (
    SELECT
        SUBSTR(area, 1, 2) AS pref_code,
        value AS daytime_official
    FROM {{ ref('stg_pref_population') }}
    WHERE cat01 = 'A6107'
        AND year = 2020
        AND area LIKE '__000'
        AND area <> '00000'
),

factor AS (
    SELECT
        o.pref_code,
        o.daytime_official / NULLIF(SUM(r.daytime_raw), 0) AS calibration_factor
    FROM official AS o
    JOIN raw_estimate AS r ON r.pref_code = o.pref_code
    GROUP BY o.pref_code, o.daytime_official
),

calibrated AS (
    SELECT
        r.*,
        r.daytime_raw * COALESCE(f.calibration_factor, 1) AS daytime_calibrated
    FROM raw_estimate AS r
    LEFT JOIN factor AS f ON r.pref_code = f.pref_code
)

SELECT
    mesh_code,
    SUM(nighttime_population) AS nighttime_population,
    CAST(ROUND(SUM(daytime_calibrated)) AS BIGINT) AS daytime_population,
    SUM(daytime_raw) AS daytime_population_uncalibrated,
    SUM(non_labor_force) AS non_labor_force,
    SUM(unemployed) AS unemployed,
    SUM(employed_residents) AS employed_residents,
    SUM(primary_industry_residents) AS primary_industry_residents,
    SUM(residents_working_or_studying) AS residents_working_or_studying,
    SUM(preschool_children) AS preschool_children,
    SUM(resident_students) AS resident_students,
    SUM(resident_students_compulsory) AS resident_students_compulsory,
    SUM(students_commuting_out) AS students_commuting_out,
    SUM(establishments) AS establishments,
    SUM(employees_total) AS employees_total,
    SUM(employees_secondary) AS employees_secondary,
    SUM(employees_tertiary) AS employees_tertiary
FROM calibrated
GROUP BY mesh_code
