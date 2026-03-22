WITH all_items AS (
    SELECT DISTINCT cat01, item_name, unit FROM {{ ref('stg_pref_population') }}
    UNION ALL
    SELECT DISTINCT cat01, item_name, unit FROM {{ ref('stg_pref_land') }}
    UNION ALL
    SELECT DISTINCT cat01, item_name, unit FROM {{ ref('stg_pref_economy') }}
    UNION ALL
    SELECT DISTINCT cat01, item_name, unit FROM {{ ref('stg_pref_administration') }}
    UNION ALL
    SELECT DISTINCT cat01, item_name, unit FROM {{ ref('stg_pref_education') }}
    UNION ALL
    SELECT DISTINCT cat01, item_name, unit FROM {{ ref('stg_pref_labor') }}
    UNION ALL
    SELECT DISTINCT cat01, item_name, unit FROM {{ ref('stg_pref_culture') }}
    UNION ALL
    SELECT DISTINCT cat01, item_name, unit FROM {{ ref('stg_pref_housing') }}
    UNION ALL
    SELECT DISTINCT cat01, item_name, unit FROM {{ ref('stg_pref_health') }}
    UNION ALL
    SELECT DISTINCT cat01, item_name, unit FROM {{ ref('stg_pref_welfare') }}
    UNION ALL
    SELECT DISTINCT cat01, item_name, unit FROM {{ ref('stg_pref_safety') }}
    UNION ALL
    SELECT DISTINCT cat01, item_name, unit FROM {{ ref('stg_municipal_population') }}
    UNION ALL
    SELECT DISTINCT cat01, item_name, unit FROM {{ ref('stg_municipal_land') }}
    UNION ALL
    SELECT DISTINCT cat01, item_name, unit FROM {{ ref('stg_municipal_economy') }}
    UNION ALL
    SELECT DISTINCT cat01, item_name, unit FROM {{ ref('stg_municipal_administration') }}
    UNION ALL
    SELECT DISTINCT cat01, item_name, unit FROM {{ ref('stg_municipal_education') }}
    UNION ALL
    SELECT DISTINCT cat01, item_name, unit FROM {{ ref('stg_municipal_labor') }}
    UNION ALL
    SELECT DISTINCT cat01, item_name, unit FROM {{ ref('stg_municipal_culture') }}
    UNION ALL
    SELECT DISTINCT cat01, item_name, unit FROM {{ ref('stg_municipal_housing') }}
    UNION ALL
    SELECT DISTINCT cat01, item_name, unit FROM {{ ref('stg_municipal_health') }}
    UNION ALL
    SELECT DISTINCT cat01, item_name, unit FROM {{ ref('stg_municipal_welfare') }}
    UNION ALL
    SELECT DISTINCT cat01, item_name, unit FROM {{ ref('stg_municipal_safety') }}
)

SELECT DISTINCT
    cat01 AS item_code,
    item_name,
    CASE LEFT(cat01, 1)
        WHEN 'A' THEN 'A 人口・世帯'
        WHEN 'B' THEN 'B 自然環境'
        WHEN 'C' THEN 'C 経済基盤'
        WHEN 'D' THEN 'D 行政基盤'
        WHEN 'E' THEN 'E 教育'
        WHEN 'F' THEN 'F 労働'
        WHEN 'G' THEN 'G 文化・スポーツ'
        WHEN 'H' THEN 'H 居住'
        WHEN 'I' THEN 'I 健康・医療'
        WHEN 'J' THEN 'J 福祉・社会保障'
        WHEN 'K' THEN 'K 安全'
    END AS domain,
    LEFT(cat01, 2) AS subdomain_code,
    LEFT(cat01, 3) AS item_group_code,
    unit
FROM all_items
