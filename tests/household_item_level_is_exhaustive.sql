-- 家計調査の item_level が全行に付いていることを検証する。
-- 結果が0行ならテスト成功。行が返る場合、e-Stat のメタ情報から level が消えている。
--
-- 品目は総額・費目・品目が同じ列に混在していて、絞り込みの唯一の手掛かりが
-- item_level と item_parent しかない。level が NULL で入ってきても行数も値も
-- 変わらないので黙って通ってしまう。ここで落とす。

{% set household_marts = ['expenditure', 'quantity'] %}

{% for mart in household_marts %}
SELECT DISTINCT '{{ mart }}' AS model_name, cat01, item_name
FROM {{ ref(mart) }}
WHERE item_level IS NULL
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
