-- 団体コード(6桁・全国地方公共団体コード)から標準地域コード(5桁)を切り出す。
-- 本データセットの census / boundary / code は 5桁側の体系なので、そこで結合する。
-- 島しょ集計行と2009年の北方領土6村はコードを持たないため NULL のまま残る。
SELECT
    reference_date,
    year,
    resident_kind,
    area_level,
    lg_code,
    LEFT(lg_code, 5) AS area_code,
    pref_name,
    municipality_name,
    population_male,
    population_female,
    population_total,
    households,
    moved_in_domestic,
    moved_in_overseas,
    moved_in_total,
    births,
    other_added,
    total_added,
    moved_out_domestic,
    moved_out_overseas,
    moved_out_total,
    deaths,
    other_removed,
    total_removed,
    net_change,
    natural_change,
    social_change
FROM {{ ref('raw_resident_population') }}
