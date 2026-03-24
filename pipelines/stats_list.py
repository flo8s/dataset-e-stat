"""統計表カタログ取得 (getStatsList)。"""

import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List

import dlt
from estat_api_dlt_helper.api.client import EstatApiClient

from pipelines import EstatStatus

logger = logging.getLogger(__name__)

CACHE_FILE = Path(".stats_list_cache.json")


def _fetch(client: EstatApiClient, **kwargs) -> list[dict]:
    """getStatsList API を呼び、TABLE_INF のリストを返す。"""
    result = client.get_stats_list(**kwargs)

    stats_list = result.get("GET_STATS_LIST", {})
    status = stats_list.get("RESULT", {}).get("STATUS")
    if status not in (EstatStatus.OK, EstatStatus.PARTIAL):
        error_msg = stats_list.get("RESULT", {}).get("ERROR_MSG", "Unknown error")
        raise RuntimeError(f"stats_list: API error (status {status}): {error_msg}")

    datalist = stats_list.get("DATALIST_INF", {})
    tables = datalist.get("TABLE_INF", [])
    if isinstance(tables, dict):
        tables = [tables]

    return tables


def _load_cache(ttl_hours: int = 24) -> list[dict] | None:
    """キャッシュが有効なら読み込む。TTL 切れまたは未作成なら None。"""
    if not CACHE_FILE.exists():
        return None
    data = json.loads(CACHE_FILE.read_text())
    cached_at = datetime.fromisoformat(data["cached_at"])
    if datetime.now() - cached_at > timedelta(hours=ttl_hours):
        logger.info("stats_list cache expired")
        return None
    logger.info(f"stats_list: using cache ({len(data['tables'])} tables)")
    return data["tables"]


def _save_cache(tables: list[dict]):
    """キャッシュに保存する。"""
    CACHE_FILE.write_text(
        json.dumps(
            {
                "cached_at": datetime.now().isoformat(),
                "tables": tables,
            }
        )
    )
    logger.info(f"stats_list: cached {len(tables)} tables")


@dlt.resource(name="stats_list", write_disposition="replace")
def stats_list_resource(app_id: str):
    """全統計表メタデータを取得してロードする。"""
    client = EstatApiClient(app_id=app_id, timeout=300)
    tables = _fetch(client)
    logger.info(f"stats_list: {len(tables)} tables")
    yield tables


def fetch_all_ids(app_id: str, cache_ttl_hours: int | None = None) -> List[str]:
    """全統計表のユニーク ID リストを返す。cache_ttl_hours 指定時はキャッシュを使う。"""
    tables = (
        _load_cache(ttl_hours=cache_ttl_hours) if cache_ttl_hours is not None else None
    )
    if tables is None:
        client = EstatApiClient(app_id=app_id, timeout=300)
        tables = _fetch(client)
        _save_cache(tables)
    ids = list(set(t["@id"] for t in tables))
    logger.info(f"stats_list: {len(ids)} unique tables")
    return ids


def fetch_updated_ids(app_id: str, days: int = 30) -> List[str]:
    """直近 N 日間に更新された統計表の ID を返す。

    e-Stat API の入出力で日付形式が異なる:
      リクエスト (updatedDate パラメータ): yyyymmdd-yyyymmdd
      レスポンス (UPDATED_DATE フィールド): yyyy-mm-dd
    """
    client = EstatApiClient(app_id=app_id, timeout=300)
    since = (date.today() - timedelta(days=days)).strftime("%Y%m%d")
    today = date.today().strftime("%Y%m%d")
    tables = _fetch(client, updatedDate=f"{since}-{today}")
    ids = list(set(t["@id"] for t in tables))
    logger.info(f"stats_list: {len(ids)} tables updated in last {days} days")
    return ids
