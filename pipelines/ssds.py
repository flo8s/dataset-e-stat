"""社会・人口統計体系データ取得 (getStatsData)。"""

import dlt
import pyarrow as pa
from estat_api_dlt_helper import estat_source, estat_table


def _drop_stat_inf(table: pa.Table) -> pa.Table:
    """行ごとに複製される stat_inf (統計表メタ) 列を落とす。

    estat_api_dlt_helper は全行に同一の TABLE_INF struct (統計表名・調査日等、
    約670バイト/行) を複製して載せる。stg / mart では未使用で、巨大テーブルでは
    これがロード時の DuckDB メモリを支配し OOM の原因になるため除去する。
    """
    if "stat_inf" in table.column_names:
        return table.drop_columns(["stat_inf"])
    return table


def _build_table(app_id: str, t: dict):
    resource = estat_table(
        stats_data_id=t["statsDataId"],
        table_name=t["name"],
        write_disposition="merge" if t.get("merge_keys") else "replace",
        primary_key=t.get("merge_keys"),
        app_id=app_id,
        incremental=dlt.sources.incremental("time", initial_value="0000000000")
        if t.get("incremental")
        else None,
    )
    if t.get("slim_metadata"):
        resource = resource.add_map(_drop_stat_inf)
    return resource


def create_source(app_id: str, tables_config: dict):
    """tables.yml の設定から SSDS データソースを作成する。"""
    return estat_source(
        app_id=app_id,
        tables=[_build_table(app_id, t) for t in tables_config["tables"]],
    )
