-- census_municipality の area_level が全行に付いていることを検証する。
-- 結果が0行ならテスト成功。行が返る場合、e-Stat が想定外の level を配り始めている。
--
-- 小地域の area_level が桁数から導けるのに対し、市区町村・都道府県の area は
-- 全て5桁で、粒度は e-Stat のメタ情報が持つ level(1/2/4/5/6/7)にしか無い。
-- 新しい level が増えると CASE がどれにも当たらず NULL が混ざるが、行数も値も
-- 変わらないので黙って通ってしまう。ここで落とす。

SELECT area, area_name, area_level
FROM {{ ref('census_municipality') }}
WHERE area_level IS NULL
