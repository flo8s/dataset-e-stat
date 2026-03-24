"""e-Stat データパイプライン。

1. stats_list: 統計表カタログ取得 (getStatsList)
2. meta_info:  メタ情報取得 (getMetaInfo) — 直近更新分のみ
3. ssds:       社会・人口統計体系データ取得 (getStatsData)
4. dbt:        dbt ビルド
"""

import os
from pathlib import Path

import yaml
from dbt.cli.main import dbtRunner

from pipelines import create_pipeline
from pipelines.meta_info import meta_info_resource
from pipelines.ssds import create_source
from pipelines.stats_list import fetch_updated_ids, stats_list_resource


def dbt_build():
    dbt = dbtRunner()

    result = dbt.invoke(["deps"])
    if not result.success:
        raise SystemExit("dbt deps failed")

    result = dbt.invoke(["run"])
    if not result.success:
        raise SystemExit("dbt run failed")

    result = dbt.invoke(["docs", "generate"])
    if not result.success:
        raise SystemExit("dbt docs generate failed")


def main():
    with open(Path(__file__).parent / "tables.yml") as f:
        tables_config = yaml.safe_load(f)

    pipeline = create_pipeline()
    app_id = os.environ["ESTAT_API_KEY"]

    # 1. 統計表カタログ (全件取得)
    pipeline.run(stats_list_resource(app_id))

    # 2. メタ情報 (直近30日間に更新された統計表のみ)
    updated_ids = fetch_updated_ids(app_id, days=30)
    if updated_ids:
        pipeline.run(meta_info_resource(app_id, updated_ids))

    # 3. 社会・人口統計体系(SSDS) データ
    pipeline.run(create_source(app_id, tables_config))

    # 4. dbt ビルド
    dbt_build()


if __name__ == "__main__":
    main()
