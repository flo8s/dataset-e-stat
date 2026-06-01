"""e-Stat データパイプライン。

1. census_boundary: 国勢調査境界データ取得 (Shapefile DL)
2. stats_list:      統計表カタログ取得 (getStatsList)
3. meta_info:       メタ情報取得 (getMetaInfo) — 直近更新分のみ
4. ssds:            社会・人口統計体系データ取得 (getStatsData)
5. dbt build
6. snapshot Neon catalog (e_stat schema) to R2
"""

from __future__ import annotations

import importlib.util
import logging
import os
import sys
from pathlib import Path

import yaml
from dbt.cli.main import dbtRunner

from pipelines import create_pipeline
from pipelines.census_boundary import download_boundary
from pipelines.meta_info import meta_info_resource
from pipelines.ssds import create_source
from pipelines.stats_list import fetch_updated_ids, stats_list_resource

logger = logging.getLogger("pipelines")

SHARED_SCRIPTS = Path(__file__).resolve().parent / "shared" / "scripts"
_spec = importlib.util.spec_from_file_location(
    "snapshot_to_r2", SHARED_SCRIPTS / "snapshot-to-r2.py"
)
assert _spec and _spec.loader
snapshot_to_r2 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(snapshot_to_r2)


def main() -> None:
    target = os.environ.get("DBT_TARGET", sys.argv[1] if len(sys.argv) > 1 else "default")

    with open(Path(__file__).parent / "tables.yml") as f:
        tables_config = yaml.safe_load(f)

    logger.info("1/6: census_boundary (国勢調査境界データ)")
    download_boundary("data/census_boundary")

    pipeline = create_pipeline()
    app_id = os.environ["ESTAT_API_KEY"]

    logger.info("2/6: stats_list (統計表カタログ)")
    info = pipeline.run(stats_list_resource(app_id))
    logger.info(f"  {info}")

    logger.info("3/6: meta_info (メタ情報)")
    updated_ids = fetch_updated_ids(app_id, days=3)
    if updated_ids:
        info = pipeline.run(meta_info_resource(app_id, updated_ids))
        logger.info(f"  {info}")
    else:
        logger.info("  skip (no updates)")

    logger.info("4/6: ssds (社会・人口統計体系)")
    info = pipeline.run(create_source(app_id, tables_config))
    logger.info(f"  {info}")

    logger.info("5/6: dbt build")
    dbt = dbtRunner()
    for cmd in (
        ["deps"],
        ["build", "--target", target],
        ["docs", "generate", "--target", target],
    ):
        result = dbt.invoke(cmd)
        if not result.success:
            raise SystemExit(f"dbt {' '.join(cmd)} failed")

    logger.info("6/6: snapshot to R2")
    snapshot_to_r2.run(target)


if __name__ == "__main__":
    main()
