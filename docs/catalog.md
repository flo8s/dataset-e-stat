---
title: 統計表カタログ・指標定義
order: 13
---

# 統計表カタログと指標定義

どんな統計表・指標が収録されているかを調べるためのメタデータです。

- `e_stat.main.stats_catalog`: 統計表のカタログ（政府統計名・分野・集計地域区分などで横断検索）
- `e_stat.ssds.item_catalog`: 社会・人口統計体系の指標定義（`item_code` が各カテゴリテーブルの `cat01`）

## 統計表を探す: stats_catalog

```sql
SELECT stat_name, table_title, gov_org_name, collect_area, cycle
FROM e_stat.main.stats_catalog
WHERE table_title LIKE '%人口%'
LIMIT 20
```

主なカラム: `stats_data_id`（統計表ID）, `stat_name`（政府統計名）, `gov_org_name`（作成機関）, `table_title`（統計表題名）, `main_category` / `sub_category`（統計分野）, `collect_area`（集計地域区分）, `cycle`（周期）。

## 指標コードを探す: item_catalog

SSDS の各カテゴリテーブルで使う `cat01` は、ここで `item_name` を検索して特定します。`item_code` がそのまま `cat01` です。`#` で始まる item_code は算出指標の定義で、各カテゴリテーブルには収録されていないため除外します。

```sql
SELECT DISTINCT table_title, item_code, item_name, unit
FROM e_stat.ssds.item_catalog
WHERE item_name LIKE '%就業者%'
  AND item_code NOT LIKE '#%'
ORDER BY item_code
LIMIT 20
```

## 分野ごとの指標数

```sql
SELECT table_title, COUNT(DISTINCT item_code) AS item_count
FROM e_stat.ssds.item_catalog
WHERE item_code NOT LIKE '#%'
GROUP BY table_title
ORDER BY item_count DESC
```
