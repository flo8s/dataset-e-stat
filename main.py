"""e-Stat データパイプライン。

1. census_boundary:    国勢調査境界データ取得 (Shapefile DL)
2. municipality_code:  統計に用いる標準地域コード取得 (総務省統計局 CSV/Excel DL)
3. stats_list:         統計表カタログ取得 (getStatsList)
4. meta_info:          メタ情報取得 (getMetaInfo) — 直近更新分のみ
5. ssds:               社会・人口統計体系データ取得 (getStatsData)
6. census_small_area:  国勢調査小地域(町丁・字等)統計取得 (searchKind=2 + getStatsData)
7. dbt:                dbt ビルド
"""

import logging
import os
from pathlib import Path

import yaml
from dbt.cli.main import dbtRunner

from pipelines import create_pipeline
from pipelines.census_boundary import download_boundary
from pipelines.census_small_area import (
    SMALL_AREA_TABLES,
    create_small_area_source,
    fetch_small_area_ids,
)
from pipelines.meta_info import meta_info_resource
from pipelines.municipality_code import build_municipality_code
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
    logger.info("1/7: census_boundary (国勢調査境界データ)")
    download_boundary("data/census_boundary")

    # 2. 統計に用いる標準地域コード (総務省統計局 CSV/Excel DL、API 不要)
    logger.info("2/7: municipality_code (統計に用いる標準地域コード)")
    build_municipality_code("data/municipality_code")

    pipeline = create_pipeline()
    app_id = os.environ["ESTAT_API_KEY"]

    # 3. 統計表カタログ (全件取得)
    logger.info("3/7: stats_list (統計表カタログ)")
    info = pipeline.run(stats_list_resource(app_id))
    logger.info(f"  {info}")

    # 4. メタ情報 (直近3日間に更新された統計表のみ)
    logger.info("4/7: meta_info (メタ情報)")
    updated_ids = fetch_updated_ids(app_id, days=3)
    if updated_ids:
        info = pipeline.run(meta_info_resource(app_id, updated_ids))
        logger.info(f"  {info}")
    else:
        logger.info("  skip (no updates)")

    # 5. 社会・人口統計体系(SSDS) データ
    logger.info("5/7: ssds (社会・人口統計体系)")
    info = pipeline.run(create_source(app_id, tables_config))
    logger.info(f"  {info}")

    # 6. 国勢調査 小地域(町丁・字等)統計データ
    logger.info("6/7: census_small_area (小地域統計)")
    for spec in SMALL_AREA_TABLES:
        ids = fetch_small_area_ids(app_id, spec["title_prefix"])
        if ids:
            info = pipeline.run(
                create_small_area_source(
                    app_id, ids, spec["name"], spec["primary_key"]
                )
            )
            logger.info(f"  {spec['name']}: {info}")
        else:
            logger.info(f"  skip {spec['name']} (no tables)")

    # 7. dbt ビルド
    logger.info("7/7: dbt build")
    dbt_build()


if __name__ == "__main__":
    main()
