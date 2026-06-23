---
title: 小地域境界（国勢調査）
order: 14
---

# 町丁・字等別境界データ（small_area）

令和2年国勢調査の町丁・字等別の境界ポリゴンと、人口・世帯数を収録します。テーブルは `e_stat.boundary.small_area`。境界と集計データは `key_code` で結合できる形になっています。

水面調査区は除外、同一 `key_code` に複数境界がある場合は代表（飛び地等を除外）のみを残しています。

## カラム構成

- prefecture_name / city_name / area_name: 都道府県名 / 市区町村名 / 町丁・字等名
- key_code: 図形と集計データのリンクコード
- jinko: 人口
- setai: 世帯数
- area_m2: 面積（図郭による算出値。公式面積とは一致しない）
- x_code / y_code: 代表点の経度 / 緯度（10進）
- geometry: 境界ポリゴン（GEOMETRY 型、CRS: EPSG:4612 JGD2011）

`geometry` は空間型のため、テキストで確認したいときは `ST_AsText(geometry)` を使います。緯度経度だけなら `x_code` / `y_code` が手軽です。

## 市区町村別の人口集計

```sql
SELECT city_name, SUM(jinko) AS population, COUNT(*) AS areas
FROM e_stat.boundary.small_area
WHERE prefecture_name = '東京都'
GROUP BY city_name
ORDER BY population DESC
LIMIT 10
```

## 特定の市区町村の町丁別人口

```sql
SELECT area_name, jinko, setai, x_code, y_code
FROM e_stat.boundary.small_area
WHERE city_name = 'つくば市'
ORDER BY jinko DESC
LIMIT 20
```

## 境界ポリゴンを取得（GIS 用途）

```sql
SELECT area_name, jinko, ST_AsText(geometry) AS wkt
FROM e_stat.boundary.small_area
WHERE city_name = 'つくば市'
ORDER BY jinko DESC
LIMIT 5
```
