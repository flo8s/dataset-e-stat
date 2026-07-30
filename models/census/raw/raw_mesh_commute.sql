{# 令和2年国勢調査 1kmメッシュ 従業地・通学地および教育 (statsId T001181)。
   従業地・通学地の項目は「当地に常住する」= 常住地ベースの流出側しかない
   (従業地ベースの流入はメッシュでも小地域でも公表されていない)。
     T001181037 当地に常住する就業者・通学者 総数 (15歳以上、自宅従業を含む)
     T001181039 当地に常住する通学者数 (15歳以上)
     T001181058 未就学者 総数
     T001181076 在学者 小学校・中学校 総数
     T001181073 在学者 総数 (検証用) #}
SELECT
    KEY_CODE AS mesh_code,
    -- 配布は都道府県別で、県境をまたぐメッシュは各県のファイルに
    -- その県の分だけが入る。集約は stg で行う。
    REGEXP_EXTRACT(filename, '_(\d{2})\.csv$', 1) AS pref_code,
    T001181037 AS residents_working_or_studying,
    T001181039 AS students_commuting_out,
    T001181058 AS preschool_children,
    T001181073 AS resident_students,
    T001181076 AS resident_students_compulsory
FROM read_csv(
    'data/mesh_stats/T001181_*.csv',
    header = true,
    all_varchar = true,
    filename = true
)
