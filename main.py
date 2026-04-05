"""e-Stat データパイプライン。

1. census_boundary: 国勢調査境界データ取得 (Shapefile DL)
2. stats_list:      統計表カタログ取得 (getStatsList)
3. meta_info:       メタ情報取得 (getMetaInfo) — 直近更新分のみ
4. ssds:            社会・人口統計体系データ取得 (getStatsData)
5. dbt:             dbt ビルド
"""

import logging
import os
from pathlib import Path

import yaml
from dbt.cli.main import dbtRunner

from pipelines import create_pipeline
from pipelines.census_boundary import download_boundary
from pipelines.meta_info import meta_info_resource
from pipelines.ssds import create_source
from pipelines.stats_list import fetch_updated_ids, stats_list_resource

logger = logging.getLogger("pipelines")


def dbt_build():
    dbt = dbtRunner()

    result = dbt.invoke(["deps"])
    if not result.success:
        raise SystemExit("dbt deps failed")

    result = dbt.invoke(["build"])
    if not result.success:
        raise SystemExit("dbt build failed")

    result = dbt.invoke(["docs", "generate"])
    if not result.success:
        raise SystemExit("dbt docs generate failed")


def main():
    with open(Path(__file__).parent / "tables.yml") as f:
        tables_config = yaml.safe_load(f)

    # 1. 国勢調査境界データ (Shapefile DL)
    logger.info("1/5: census_boundary (国勢調査境界データ)")
    download_boundary("data/census_boundary")

    pipeline = create_pipeline()
    app_id = os.environ["ESTAT_API_KEY"]

    # 2. 統計表カタログ (全件取得)
    logger.info("2/5: stats_list (統計表カタログ)")
    info = pipeline.run(stats_list_resource(app_id))
    logger.info(f"  {info}")

    # 3. メタ情報 (直近30日間に更新された統計表のみ)
    logger.info("3/5: meta_info (メタ情報)")
    updated_ids = fetch_updated_ids(app_id, days=30)
    if updated_ids:
        info = pipeline.run(meta_info_resource(app_id, updated_ids))
        logger.info(f"  {info}")
    else:
        logger.info("  skip (no updates)")

    # 4. 社会・人口統計体系(SSDS) データ
    logger.info("4/5: ssds (社会・人口統計体系)")
    info = pipeline.run(create_source(app_id, tables_config))
    logger.info(f"  {info}")

    # 5. dbt ビルド
    logger.info("5/5: dbt build")
    dbt_build()


if __name__ == "__main__":
    main()
