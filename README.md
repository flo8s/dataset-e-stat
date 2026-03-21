## データ出典

[e-Stat（政府統計の総合窓口）](https://www.e-stat.go.jp/)の API から取得した「社会・人口統計体系」のデータです。
都道府県別・市区町村別に、以下の11カテゴリの統計指標を収録しています。

## カテゴリ一覧

| カテゴリ | 都道府県テーブル | 市区町村テーブル |
|---------|---------------|---------------|
| A 人口・世帯 | pref_population | municipal_population |
| B 自然環境 | pref_land | municipal_land |
| C 経済基盤 | pref_economy | municipal_economy |
| D 行政基盤 | pref_administration | municipal_administration |
| E 教育 | pref_education | municipal_education |
| F 労働 | pref_labor | municipal_labor |
| G 文化・スポーツ | pref_culture | municipal_culture |
| H 居住 | pref_housing | municipal_housing |
| I 健康・医療 | pref_health | municipal_health |
| J 福祉・社会保障 | pref_welfare | municipal_welfare |
| K 安全 | pref_safety | municipal_safety |

## テーブル構造

全テーブル共通のカラム構成です。

- cat01: 分類事項コード
- item_name: 分類事項名（例: 「総人口」「出生数」）
- area / area_name: 地域コード / 地域名
- time / time_name: 時間軸コード / 時間軸名（例: 「2020年」）
- unit: 単位（例: 「人」「km2」）
- value: 統計値

## ライセンス

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
