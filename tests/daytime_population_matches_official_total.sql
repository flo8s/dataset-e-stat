-- 1kmメッシュ昼間人口の推計が、全国合計で公表値と一致することを検証する。
-- stg_mesh_daytime_population が都道府県ごとに公表昼間人口（社会・人口統計体系
-- A6107、令和2年）へ合わせて補正しているので、その総和である全国合計も一致するはず。
-- メッシュ単位で整数に丸めているぶんのずれだけを許容する（0.05%）。
--
-- 一部の都道府県だけダウンロードに失敗した場合もここで落ちる。
WITH estimated AS (
    SELECT SUM(daytime_population) AS daytime_estimated
    FROM {{ ref('daytime_population_mesh_1km') }}
),

official AS (
    SELECT value AS daytime_official
    FROM {{ ref('stg_pref_population') }}
    WHERE cat01 = 'A6107' AND year = 2020 AND area = '00000'
)

SELECT
    o.daytime_official,
    e.daytime_estimated
FROM official AS o
CROSS JOIN estimated AS e
WHERE ABS(e.daytime_estimated - o.daytime_official) > o.daytime_official * 0.0005
