-- establishment_industry の area_level が全行に付いていることを検証する。
-- 結果が0行ならテスト成功。行が返る場合、e-Stat が想定外の level を配り始めている。
--
-- 経済センサスの area は全て5桁で、粒度は e-Stat のメタ情報が持つ level(1/2/3)に
-- しか無い。国勢調査の市区町村別集計とは level の振り方が違い(全国と都道府県が
-- どちらも level=1)、専用のマクロで振り分けている。新しい level が増えると CASE が
-- どれにも当たらず NULL が混ざるが、行数も値も変わらないので黙って通ってしまう。

SELECT area, area_name, area_level
FROM {{ ref('establishment_industry') }}
WHERE area_level IS NULL
