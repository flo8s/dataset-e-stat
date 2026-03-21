"""dlt ingestion + dbt ビルド + メタデータ生成パイプライン。"""

import logging
import os
from pathlib import Path

import dlt
import yaml
from dbt.cli.main import dbtRunner
from estat_api_dlt_helper import estat_source, estat_table

from fdl.ducklake import create_destination

# dlt の ArrowExtractor が merge 時に出す column hints 差異の WARNING を抑制
logging.getLogger("dlt.extract.extractors").setLevel(logging.ERROR)
SOURCE_SCHEMA = "_source"


def main():
    # ingest
    # e-Stat API からデータを取得し dlt 経由で DuckLake に書き込む。
    with open(Path(__file__).parent / "tables.yml") as f:
        tables_config = yaml.safe_load(f)

    destination = create_destination(os.environ.get("DUCKLAKE_STORAGE", "dist"))

    pipeline = dlt.pipeline(
        pipeline_name="estat",
        destination=destination,
        dataset_name=SOURCE_SCHEMA,
    )
    app_id = os.environ["ESTAT_API_KEY"]
    source = estat_source(
        app_id=app_id,
        tables=[
            estat_table(
                stats_data_id=t["statsDataId"],
                table_name=t["name"],
                write_disposition="merge" if t.get("merge_keys") else "replace",
                primary_key=t.get("merge_keys"),
                app_id=app_id,
                incremental=dlt.sources.incremental("time", initial_value="0000000000")
                if t.get("incremental")
                else None,
            )
            for t in tables_config["tables"]
        ],
    )
    info = pipeline.run(source)
    print(info)

    # dbt ビルド
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


if __name__ == "__main__":
    main()
