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

- cat01: 分類事項コード（例: 「J1102」）
- item_name: 分類事項名（例: 「J1102_現に保護を受けた生活保護被保護実世帯数」）
- cat01_level: 分類事項の階層レベル（1 が最上位）
- cat01_parent: 1つ上の分類事項コード（最上位は NULL）
- area / area_name: 地域コード / 地域名
- time_name: 時間軸名（例: 「2020年度」）
- year: time_name から抽出した西暦4桁
- unit: 単位（例: 「人」「km2」）
- value: 統計値

上位項目と内訳が同じ列に混在します。`cat01_parent` で切り分けられます。

```sql
-- 最上位の指標だけ
SELECT * FROM e_stat.ssds.j_pref_welfare WHERE cat01_parent IS NULL;

-- 生活保護被保護実世帯数（J1102）の内訳だけ
SELECT * FROM e_stat.ssds.j_pref_welfare WHERE cat01_parent = 'J1102';
```

ただし内訳が親を分割しているとは限りません。A 人口・世帯の男女別のように合計が親に
一致する系統もあれば、J1102 のように「うち」項目だけが並んでいて親に届かない系統も
あります。レベルを揃えれば安全に合計できる、とは考えないでください。

地域も同様に、都道府県版は全国計（area = 00000）を、市区町村版は政令指定都市の
合計行と行政区をどちらも含みます。市区町村単位で数えるときは
`code.municipality` の `is_municipality` で絞ります。

## 国勢調査 小地域集計（census スキーマ）

令和2年国勢調査の町丁・字等別（小地域）集計です。area は同データセットの境界データ
small_area の key_code と同一体系で、`key_code = area` で境界ポリゴンと結合できます。
area には市区町村（5桁）・町丁・字等（9桁）・その内訳区画（11桁）の3階層が含まれ、
5桁は9桁の合計、9桁は11桁の合計にあたります。桁数を揃えずに合計すると重複します。
分類は cat01（主分類）、cat02（秘匿・合算区分: 無し/合算/秘匿）です。

| テーブル | 内容 | cat01 | value |
|---------|------|-------|-------|
| census_small_area_age | 年齢（5歳階級・4区分）別、男女別人口 | 男女・年齢区分 | 人口 |
| census_small_area_household | 世帯の家族類型別一般世帯数 | 家族類型 | 一般世帯数 |
| census_small_area_industry | 産業（大分類）別就業者数 | 産業大分類 | 就業者数 |
| census_small_area_housing | 住宅の所有の関係別一般世帯数 | 住宅の種類・所有の関係 | 一般世帯数 |

4表とも cat02 は秘匿・合算区分です。census_small_area_age だけ名称列が sex という
名前ですが、中身は他の3表の secrecy と同じ秘匿・合算区分で、男女は cat01 側
（総数 / 男 / 女 × 年齢区分）に入っています。主分類の名称列はテーブルごとに
age_class / family_type / industry / tenure として展開しています。

秘匿（cat02 = 3）の行は value が 0 で入りますが「0」ではなく非公表で、実数は
合算（cat02 = 2）の地域に含まれています。そのまま合計すると実態より小さくなります。

出典: 総務省統計局 令和2年国勢調査 小地域集計（統計GIS）。https://www.e-stat.go.jp/gis

## 統計に用いる標準地域コード（code スキーマ）

都道府県・市区町村を 5 桁で表す「統計に用いる標準地域コード」の現行一覧と、
その変更（廃置分合）履歴です。area は census / boundary / SSDS の各テーブルが
用いる地域コードと同一体系で、コードから名称を引くマスタとして使えます。
全国地方公共団体コード（6 桁・チェックデジット付き）とは別体系です。

| テーブル | 内容 | 主なカラム |
|---------|------|-----------|
| municipality | 現行の標準地域コード一覧 | area_code / pref_name / area_kind / is_municipality / municipality_code / municipality_name |
| municipality_change | コード変更（廃置分合）履歴 | effective_date / old_code / new_code / is_code_deleted / reason |

### 階層の判別

municipality は都道府県・政令指定都市・行政区・郡/振興局/支庁・市区町村を同一テーブルに
収録しています。どの階層の行かは `area_kind` で判別できます。

| area_kind | 件数 | 内容 |
|-----------|-----:|------|
| prefecture | 47 | 都道府県の合計行 |
| designated_city | 20 | 政令指定都市（行政区の親にあたる行） |
| ward | 171 | 政令指定都市の行政区 |
| district | 326 | 郡・振興局・支庁・特別区部（集計用の親行） |
| municipality | 1,727 | 市区町村 |

市区町村として数える行は `is_municipality` が真の行（1,747件）です。政令指定都市を
1団体として数え、行政区は数えません。

```sql
SELECT * FROM municipality WHERE is_municipality;
```

東京の特別区23は市区町村として数え、その親にあたる集計行「特別区部」（13100）は
数えません。同じ「区」でも行政区と特別区で扱いが逆になります。

`municipality_code` は所属する市区町村のコードです。行政区は所属市を、市区町村と
政令指定都市は自分自身を指します。行政区の粒度で来るデータを市区町村に寄せるときに、
どのコードでも同じキーで束ねられます。

```sql
-- 行政区のコードでも市区町村のコードでも、同じ市区町村に寄る
SELECT area_code, municipality_name, area_kind, municipality_code
FROM municipality
WHERE municipality_code = '22130';   -- 浜松市とその3行政区
```

`district_name` に同居していた郡名・振興局名・政令市名は `county_name` /
`subprefecture_name` / `designated_city_name` に分けています。北海道の町村は
振興局・支庁で括られていて郡名を持たないため、`county_name` は NULL になります。

municipality_change は旧コードから新コードへの対応を 1 件 1 行で収録します。編入・
合併で消滅したコードは new_name が「削除」表記になり is_code_deleted が真になります。
市区町村コード付きの時系列を合併をまたいで接続する横断キーとして使えます。
収録範囲は総務省が機械可読形式で提供する平成19年（2007年）4月2日以降の変更のみで、
平成の大合併のピーク（1999〜2006年）は含みません。

出典: 総務省統計局 統計に用いる標準地域コード。
https://www.soumu.go.jp/toukei_toukatsu/index/seido/9-5.htm

## 境界データ（boundary スキーマ）

地図化・空間集計に使う区画のポリゴンです。統計値は持ちません。

| テーブル | 内容 | 主なカラム |
|---------|------|-----------|
| small_area | 令和2年国勢調査 町丁・字等別境界 | key_code / prefecture_code / city_code / area_name / geometry |
| mesh_1km | 標準地域メッシュ 3次メッシュ（1kmメッシュ）境界 | mesh_code / mesh1_code / mesh2_code / mesh3_code / geometry |

mesh_1km は標準地域メッシュ（JIS X 0410）の区画そのものです。mesh_code は 8 桁の
3次メッシュコードで、上位から 1次メッシュ（約80km四方・4桁）、2次メッシュ
（約10km四方・2桁）、3次メッシュ（約1km四方・2桁）に分解できます。各桁は
mesh1_code / mesh2_code / mesh3_code にも分けて持たせています。同じコード体系を使う
メッシュ統計と `mesh_code` で結合して地図に載せられます。1 区画の大きさは緯度方向
30 秒・経度方向 45 秒で、南鳥島・沖ノ鳥島を含む陸域の 1次メッシュ 176 区画分
（501,600 メッシュ）を収録しています。座標系は EPSG:4612 です。

出典: 総務省統計局 統計GIS 境界データ。https://www.e-stat.go.jp/gis

## ライセンス

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
