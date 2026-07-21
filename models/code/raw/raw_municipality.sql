-- 現行の統計に用いる標準地域コード一覧 (municipality_code パイプライン生成の NDJSON)。
-- コードは先頭ゼロを保つため VARCHAR で読む。
SELECT
    area_code,
    pref_code,
    pref_name,
    district_name,
    municipality_name,
    yomigana,
    is_prefecture
FROM read_json(
    'data/municipality_code/municipality.ndjson',
    columns = {
        area_code: 'VARCHAR',
        pref_code: 'VARCHAR',
        pref_name: 'VARCHAR',
        district_name: 'VARCHAR',
        municipality_name: 'VARCHAR',
        yomigana: 'VARCHAR',
        is_prefecture: 'BOOLEAN'
    },
    format = 'newline_delimited'
)
