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

## 国勢調査 小地域集計（census スキーマ）

令和2年国勢調査の町丁・字等別（小地域）集計です。area は同データセットの境界データ
small_area の key_code と同一体系で、`key_code = area` で境界ポリゴンと結合できます。
area には市区町村（5桁）と小地域（9桁/11桁）の各階層が含まれます。
分類は cat01（主分類）、cat02（秘匿・合算区分: 無し/合算/秘匿）です。

| テーブル | 内容 | cat01 | value |
|---------|------|-------|-------|
| census_small_area_age | 年齢（5歳階級・4区分）別、男女別人口 | 年齢区分 | 人口 |
| census_small_area_household | 世帯の家族類型別一般世帯数 | 家族類型 | 一般世帯数 |
| census_small_area_industry | 産業（大分類）別就業者数 | 産業大分類 | 就業者数 |
| census_small_area_housing | 住宅の所有の関係別一般世帯数 | 住宅の種類・所有の関係 | 一般世帯数 |

census_small_area_age は cat02 が男女区分（cat02→sex）、それ以外の3表は cat02 が
秘匿・合算区分（cat02→secrecy）です。主分類の名称列はテーブルごとに age_class /
family_type / industry / tenure として展開しています。

出典: 総務省統計局 令和2年国勢調査 小地域集計（統計GIS）。https://www.e-stat.go.jp/gis

## 統計に用いる標準地域コード（code スキーマ）

都道府県・市区町村を 5 桁で表す「統計に用いる標準地域コード」の現行一覧と、
その変更（廃置分合）履歴です。area は census / boundary / SSDS の各テーブルが
用いる地域コードと同一体系で、コードから名称を引くマスタとして使えます。
全国地方公共団体コード（6 桁・チェックデジット付き）とは別体系です。

| テーブル | 内容 | 主なカラム |
|---------|------|-----------|
| municipality | 現行の標準地域コード一覧 | area_code / pref_name / district_name / municipality_name / yomigana |
| municipality_change | コード変更（廃置分合）履歴 | effective_date / old_code / new_code / is_code_deleted / reason |

municipality_change は旧コードから新コードへの対応を 1 件 1 行で収録します。編入・
合併で消滅したコードは new_name が「削除」表記になり is_code_deleted が真になります。
市区町村コード付きの時系列を合併をまたいで接続する横断キーとして使えます。
収録範囲は総務省が機械可読形式で提供する平成19年（2007年）4月2日以降の変更のみで、
平成の大合併のピーク（1999〜2006年）は含みません。

出典: 総務省統計局 統計に用いる標準地域コード。
https://www.soumu.go.jp/toukei_toukatsu/index/seido/9-5.htm

## ライセンス

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
