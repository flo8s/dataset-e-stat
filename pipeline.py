"""dlt ingestion + dbt ビルド + メタデータ生成パイプライン。"""

import logging
import os
from pathlib import Path

import dlt
import yaml
from dbt.cli.main import dbtRunner
from estat_api_dlt_helper import estat_source, estat_table
from estat_api_dlt_helper.api.client import EstatApiClient

from fdl.ducklake import create_destination

# dlt の ArrowExtractor が merge 時に出す column hints 差異の WARNING を抑制
logging.getLogger("dlt.extract.extractors").setLevel(logging.ERROR)
SOURCE_SCHEMA = "_source"


@dlt.resource(name="stat_tables", write_disposition="merge", primary_key="id")
def stat_tables_resource(
    app_id: str,
    updated_date=dlt.sources.incremental("updated_date", initial_value="20200101"),
):
    """e-Stat API の全統計表メタデータを差分取得する。"""
    client = EstatApiClient(app_id=app_id)
    result = client.get_stats_list(updatedDate=updated_date.last_value)
    yield result["GET_STATS_LIST"]["DATALIST_INF"]["TABLE_INF"]


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

    # 統計表カタログ取得
    info = pipeline.run(stat_tables_resource(app_id=app_id))
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
