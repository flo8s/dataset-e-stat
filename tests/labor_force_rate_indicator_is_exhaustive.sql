-- 率の指標名が全行に付いていることを検証する。結果が0行ならテスト成功。
--
-- e-Stat のメタ情報は3指標の名称を「労働力人口」「就業」「完全失業者」としていて、
-- 人数の表と同じ名前になる。分母も指標ごとに違う（完全失業率だけ労働力人口が分母）ので、
-- stg で指標名に置き換えている。e-Stat が指標を足すと indicator が NULL のまま入り、
-- 行数も値も変わらないので黙って通る。ここで落とす。

SELECT DISTINCT indicator_code, unit
FROM {{ ref('labor_force_rate') }}
WHERE indicator IS NULL
