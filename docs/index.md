---
title: 概要と使い方
order: 0
---

# e-Stat データセットの使い方

e-Stat（政府統計の総合窓口）の API と統計GIS から取得した政府統計データです。中心となるのは社会・人口統計体系（SSDS）で、都道府県・市区町村それぞれ11分野の統計指標を共通のテーブル構造で収録しています。加えて消費者物価指数、統計表のメタデータ、令和2年国勢調査の小地域（町丁・字等）集計と境界データを収録します。

出典: [e-Stat](https://www.e-stat.go.jp/) / [社会・人口統計体系（SSDS）](https://www.e-stat.go.jp/statistics/00200502)

## スキーマとテーブルの構成

| スキーマ | 内容 |
|---------|------|
| ssds | 社会・人口統計体系。11分野 × 都道府県/市区町村 = 22テーブル + 指標定義 item_catalog |
| cpi | 消費者物価指数 price_index |
| main | 統計表カタログ stats_catalog |
| census | 令和2年国勢調査 小地域集計 census_small_area_*（年齢・世帯・住宅・産業） |
| boundary | 令和2年国勢調査 町丁・字等別境界 small_area |

11分野（A〜K）の詳細は各カテゴリのガイドを参照してください。

## SSDS 共通のカラム構成

ssds スキーマの22テーブルはすべて同じ8カラムです。指標を `cat01`（分類事項コード）で絞り込み、`area_name`（地域）と `year`（年）で必要な行を取り出すのが基本です。

- cat01: 分類事項コード（指標を一意に識別する。例: `A1101` = 総人口）
- item_name: 分類事項名（例: `A1101_総人口`）
- area: 地域コード（全国は `00000`、都道府県は `13000` のような5桁）
- area_name: 地域名（`全国` / `東京都` など）
- time_name: 時間軸名（例: `2020年度`）
- year: 年
- unit: 単位（例: `人`）
- value: 統計値

## 重要: 同じ指標が複数行になることがある

SSDS は複数の出典統計表を束ねているため、同一の `cat01` × `area` × `year` に対して値の等しい行が複数存在することがあります。集計時は `MAX(value)` や `DISTINCT` で重ねを取り除いてください。

```sql
-- 重複の確認（同一条件で複数行が返る）
SELECT cat01, area_name, year, COUNT(*) AS rows, COUNT(DISTINCT value) AS distinct_values
FROM e_stat.ssds.a_pref_population
WHERE cat01 = 'A1101' AND area_name = '全国' AND year = 2024
GROUP BY cat01, area_name, year
```

## 指標を探す: item_catalog

どの `cat01` を使えばよいかは `item_catalog` を `item_name` で検索して調べます。`item_code` がそのまま各カテゴリテーブルの `cat01` になります。`#` で始まる item_code は算出指標の定義で、各カテゴリテーブルには収録されていないため除外します。

```sql
SELECT DISTINCT table_title, item_code, item_name, unit
FROM e_stat.ssds.item_catalog
WHERE item_name LIKE '%総人口%'
  AND item_code NOT LIKE '#%'
ORDER BY item_code
LIMIT 20
```

## 基本パターン1: 全国の時系列

`area_name = '全国'` で絞り、`year` ごとに `MAX(value)` を取ります。

```sql
SELECT year, MAX(value) AS population
FROM e_stat.ssds.a_pref_population
WHERE cat01 = 'A1101' AND area_name = '全国'
GROUP BY year
ORDER BY year
```

## 基本パターン2: 都道府県ランキング（最新年）

最新年をサブクエリで求め、`全国` を除いて並べます。この「最新年 + MAX(value)」の形はどのカテゴリでも使えます。

```sql
SELECT area_name, MAX(value) AS population
FROM e_stat.ssds.a_pref_population
WHERE cat01 = 'A1101'
  AND area_name <> '全国'
  AND year = (SELECT MAX(year) FROM e_stat.ssds.a_pref_population WHERE cat01 = 'A1101')
GROUP BY area_name
ORDER BY population DESC
LIMIT 10
```

市区町村単位で見たい場合は `a_municipal_population` 以下の `*_municipal_*` テーブルを使います。`area_name` は `茨城県 つくば市` のように県名と市区町村名が入ります。

## 分野一覧（SSDS）

| 分野 | 都道府県テーブル | 市区町村テーブル |
|------|----------------|----------------|
| A 人口・世帯 | a_pref_population | a_municipal_population |
| B 自然環境 | b_pref_land | b_municipal_land |
| C 経済基盤 | c_pref_economy | c_municipal_economy |
| D 行政基盤 | d_pref_administration | d_municipal_administration |
| E 教育 | e_pref_education | e_municipal_education |
| F 労働 | f_pref_labor | f_municipal_labor |
| G 文化・スポーツ | g_pref_culture | g_municipal_culture |
| H 居住 | h_pref_housing | h_municipal_housing |
| I 健康・医療 | i_pref_health | i_municipal_health |
| J 福祉・社会保障 | j_pref_welfare | j_municipal_welfare |
| K 安全 | k_pref_safety | k_municipal_safety |
