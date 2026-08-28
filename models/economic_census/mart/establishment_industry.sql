{# 表章項目(tab)は6項目で固定なので、縦持ちのままにせず
   地域 × 産業 × 経営組織 の1行に開く。縦持ちだと事業所と人と百万円と万円が
   同じ列に並び、絞り込みを忘れた合計が黙って通る。

   経営組織の「（別掲）外国の会社」「（別掲）法人でない団体」は総数の内数を別に
   数え直した行で、総数 = 個人 + 会社 + 会社以外の法人 には入らない。
   is_reprint で落とせるようにしておく。 #}
SELECT
    area,
    ANY_VALUE(area_name) AS area_name,
    ANY_VALUE(area_level) AS area_level,
    ANY_VALUE(parent_area) AS parent_area,
    cat01 AS industry_code,
    ANY_VALUE(industry_name) AS industry_name,
    ANY_VALUE(industry_level) AS industry_level,
    ANY_VALUE(parent_industry) AS parent_industry,
    cat02 AS organization_code,
    ANY_VALUE(organization) AS organization,
    cat02 IN ('S1', 'S2') AS is_reprint,
    CAST(MAX(value) FILTER (WHERE tab = '102-2021') AS BIGINT) AS establishments,
    CAST(MAX(value) FILTER (WHERE tab = '113-2021') AS BIGINT) AS employees,
    CAST(MAX(value) FILTER (WHERE tab = '155-2021') AS BIGINT) AS sales_million_yen,
    MAX(value) FILTER (WHERE tab = '117-2021') AS employees_per_establishment,
    MAX(value) FILTER (WHERE tab = '156-2021') AS sales_per_establishment_10k_yen,
    MAX(value) FILTER (WHERE tab = '157-2021') AS sales_per_employee_10k_yen
FROM {{ ref('stg_economic_census_industry') }}
GROUP BY area, cat01, cat02
