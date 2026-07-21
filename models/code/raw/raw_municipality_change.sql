-- 平成19年4月2日以降のコード変更 (廃置分合) 履歴 (municipality_code パイプライン生成)。
-- コードは先頭ゼロを保つため VARCHAR、施行年月日は stg で DATE 化する。
SELECT
    effective_date,
    pref_code,
    pref_name,
    old_code,
    old_name,
    new_code,
    new_name,
    is_code_deleted,
    reason
FROM read_json(
    'data/municipality_code/municipality_change.ndjson',
    columns = {
        effective_date: 'VARCHAR',
        pref_code: 'VARCHAR',
        pref_name: 'VARCHAR',
        old_code: 'VARCHAR',
        old_name: 'VARCHAR',
        new_code: 'VARCHAR',
        new_name: 'VARCHAR',
        is_code_deleted: 'BOOLEAN',
        reason: 'VARCHAR'
    },
    format = 'newline_delimited'
)
