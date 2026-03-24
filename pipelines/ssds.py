"""社会・人口統計体系データ取得 (getStatsData)。"""

import dlt
from estat_api_dlt_helper import estat_source, estat_table


def create_source(app_id: str, tables_config: dict):
    """tables.yml の設定から SSDS データソースを作成する。"""
    return estat_source(
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
