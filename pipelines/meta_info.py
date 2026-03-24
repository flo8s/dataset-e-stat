"""メタ情報取得 (getMetaInfo)。"""

import logging
from typing import List

import dlt
from estat_api_dlt_helper.api.client import EstatApiClient

from pipelines import EstatStatus

logger = logging.getLogger(__name__)


def _fetch_one(client: EstatApiClient, app_id: str, sid: str) -> list[dict]:
    """1統計表の getMetaInfo を呼び、全分類項目をリストで返す。"""
    logger.info(f"getMetaInfo: {sid}")
    resp = client.client.get(
        "https://api.e-stat.go.jp/rest/3.0/app/json/getMetaInfo",
        params={"appId": app_id, "statsDataId": sid, "lang": "J"},
    )
    meta = resp.json().get("GET_META_INFO", {})
    if meta.get("RESULT", {}).get("STATUS") != EstatStatus.OK:
        return []

    rows = []
    class_objs = meta["METADATA_INF"]["CLASS_INF"]["CLASS_OBJ"]
    for cls in class_objs:
        if "CLASS" not in cls:
            logger.debug(
                f"CLASS not found in CLASS_OBJ: sid={sid}, id={cls.get('@id')}, keys={list(cls.keys())}"
            )
            continue
        items = cls["CLASS"]
        if isinstance(items, dict):
            items = [items]
        for item in items:
            rows.append(
                {
                    "stats_data_id": sid,
                    "class_id": cls["@id"],
                    "class_name": cls["@name"],
                    "class_description": cls.get("@description", ""),
                    "item_code": item["@code"],
                    "item_name": item["@name"],
                    "level": item.get("@level", ""),
                    "unit": item.get("@unit", ""),
                    "parent_code": item.get("@parentCode", ""),
                    "add_inf": item.get("@addInf", ""),
                }
            )
    return rows


@dlt.resource(
    name="meta_info",
    write_disposition="merge",
    primary_key=["stats_data_id", "class_id", "item_code"],
)
def meta_info_resource(app_id: str, stats_data_ids: List[str]):
    """getMetaInfo API から各統計表の全分類軸メタデータを取得する。"""
    logger.info(f"Fetching meta info for {len(stats_data_ids)} tables")
    client = EstatApiClient(app_id=app_id)

    @dlt.defer
    def fetch_deferred(sid: str):
        return _fetch_one(client, app_id, sid)

    for sid in stats_data_ids:
        yield fetch_deferred(sid)
