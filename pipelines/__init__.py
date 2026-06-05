"""パイプライン共通セットアップ。"""

import logging
import logging.config
import os
import sys
from enum import IntEnum
from pathlib import Path

import dlt
from dlt.destinations import ducklake
from dlt.destinations.impl.ducklake.configuration import DuckLakeCredentials


class EstatStatus(IntEnum):
    """e-Stat API レスポンスステータスコード。

    https://www.e-stat.go.jp/api/api-info/e-stat-manual3-0#sec3
    """

    OK = 0  # 正常終了
    NO_DATA = 1  # 正常終了（該当データなし）
    PARTIAL = 2  # 正常終了（条件不一致、部分的な結果）
    # 100+ はエラー


logging.config.dictConfig(
    {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {"plain": {"format": "%(message)s"}},
        "handlers": {
            "console": {"class": "logging.StreamHandler", "formatter": "plain"}
        },
        "loggers": {
            "pipelines": {"level": "INFO", "handlers": ["console"], "propagate": False},
            "dlt.extract.extractors": {"level": "ERROR"},
        },
    }
)

SOURCE_SCHEMA = "_source"

_SHARED_SCRIPTS = Path(__file__).resolve().parent.parent / "shared" / "scripts"
sys.path.insert(0, str(_SHARED_SCRIPTS))
from queria_config import load_target  # noqa: E402


def create_pipeline():
    """dlt パイプラインを Neon Postgres DuckLake + R2 (BYOB) で構成する。

    - catalog: Neon Postgres、META_SCHEMA で dataset 分離
    - storage: R2 バケット (s3:// プロトコル + R2 endpoint で接続)
    """
    target_name = os.environ.get("DBT_TARGET", "default")
    target = load_target(target_name)

    # DuckLake へのコミットを直列化し、リモート Postgres カタログ
    # (Neon, 高レイテンシ) でのスナップショット採番競合を減らす。
    os.environ.setdefault("LOAD__WORKERS", "1")

    from dlt.common.configuration.specs import AwsCredentials
    from dlt.common.storages.configuration import FilesystemConfiguration

    bucket_url = f"s3://{target.s3_bucket}/{target.dataset}/ducklake.duckdb.files/"
    storage = FilesystemConfiguration(
        bucket_url=bucket_url,
        credentials=AwsCredentials(
            aws_access_key_id=target.s3_access_key_id,
            aws_secret_access_key=target.s3_secret_access_key,
            endpoint_url=target.s3_endpoint,
            region_name="auto",
            s3_url_style="path",
        ),
    )

    credentials = DuckLakeCredentials(
        # dlt 1.18+ では METADATA_SCHEMA を ducklake_name から導出する。
        # 既存メタデータ (Postgres スキーマ = meta_schema) と整合させるため
        # ducklake_name に meta_schema を渡す。ATTACH エイリアスも同名になり、
        # dbt profiles.yml の alias: e_stat と一致する。
        ducklake_name=target.meta_schema,
        catalog=target.neon_dsn,
        storage=storage,
    )
    # 高レイテンシのリモート Postgres カタログ (Neon) では DuckLake の
    # 楽観ロックによるコミットが衝突し、既定の retry=10 を超えて
    # `Failed to commit DuckLake transaction` /
    # `duplicate key ... ducklake_snapshot_pkey` で失敗することがある。
    # リトライ回数を引き上げて吸収する (LOAD ducklake 後に SET GLOBAL される)。
    # ref: https://github.com/duckdb/ducklake/issues/233 , /243 , /459
    credentials.global_config = {"ducklake_max_retry_count": 100}

    return dlt.pipeline(
        pipeline_name="estat",
        destination=ducklake(credentials=credentials, override_data_path=True),
        dataset_name=SOURCE_SCHEMA,
    )
