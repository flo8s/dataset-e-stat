{# 令和2年国勢調査 1kmメッシュ 年齢別・男女別人口 (statsId T001171)。
   pipelines/mesh_stats.py が UTF-8 / 1行ヘッダーに変換した CSV を読む。
   秘匿値 '*' があるので全列 VARCHAR で読み、キャストは stg で行う。 #}
SELECT
    KEY_CODE AS mesh_code,
    -- 配布は都道府県別で、県境をまたぐメッシュは各県のファイルに
    -- その県の分だけが入る。集約は stg で行う。
    REGEXP_EXTRACT(filename, '_(\d{2})\.csv$', 1) AS pref_code,
    HTKSYORI AS secrecy_kind,
    HTKSAKI AS secrecy_target,
    T001171001 AS population_total
FROM read_csv(
    'data/mesh_stats/T001171_*.csv',
    header = true,
    all_varchar = true,
    filename = true
)
