---
title: 消費者物価指数（CPI）
order: 12
---

# 消費者物価指数（price_index）

消費者物価指数（2020年基準）です。品目別の価格指数を全国・地域・都市階級別に月次／年次で収録します。テーブルは `e_stat.cpi.price_index`。

出典: [総務省統計局 消費者物価指数（2020年基準）](https://www.e-stat.go.jp/stat-search/database?statdisp_id=0003427113)

## カラム構成

- tab / tab_name: 表章項目（`1` = 指数、ほかに前月比・前年比など）
- cat01 / item_name: 品目コード / 品目名（`0001` = 総合）
- area / area_name: 地域コード / 地域名（`全国` など67区分）
- time / time_name: 時間軸。年次は `2024年`、月次は `2024年1月` の形式
- year: 年
- value: 指数（2020年 = 100）

年次データだけを取りたいときは `time_name LIKE '____年'`、月次は `time_name LIKE '%月'` で絞り込みます。

## 全国・総合指数の年次推移

```sql
SELECT year, value
FROM e_stat.cpi.price_index
WHERE tab = '1' AND cat01 = '0001' AND area_name = '全国'
  AND time_name LIKE '____年'
ORDER BY year
```

## 品目を探す

```sql
SELECT DISTINCT cat01, item_name
FROM e_stat.cpi.price_index
WHERE item_name LIKE '%米%'
ORDER BY cat01
LIMIT 20
```

## 特定品目の月次推移（直近）

```sql
SELECT time_name, value
FROM e_stat.cpi.price_index
WHERE tab = '1' AND cat01 = '0001' AND area_name = '全国'
  AND time_name LIKE '%月'
ORDER BY time DESC
LIMIT 12
```
