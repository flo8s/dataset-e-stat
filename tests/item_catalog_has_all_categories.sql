-- item_catalog が SSDS 全テーブルのデータを持つことを検証する。
-- tables.yml で定義した22テーブル（都道府県11 + 市区町村11）のうち、
-- item_catalog に存在しない stats_data_id があればテスト失敗。
WITH expected AS (
    SELECT DISTINCT id
    FROM {{ ref('stats_list') }}
    WHERE stat_code = '00200502'
      AND id IN (
        '0000010101', '0000010102', '0000010103', '0000010104', '0000010105',
        '0000010106', '0000010107', '0000010108', '0000010109', '0000010110',
        '0000010111',
        '0000020201', '0000020202', '0000020203', '0000020204', '0000020205',
        '0000020206', '0000020207', '0000020208', '0000020209', '0000020210',
        '0000020211'
      )
),
actual AS (
    SELECT DISTINCT stats_data_id
    FROM {{ ref('item_catalog') }}
)
SELECT e.id
FROM expected e
LEFT JOIN actual a ON e.id = a.stats_data_id
WHERE a.stats_data_id IS NULL
