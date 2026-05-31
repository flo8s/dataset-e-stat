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

    return dlt.pipeline(
        pipeline_name="estat",
        destination=ducklake(
            credentials=DuckLakeCredentials(
                catalog=target.neon_dsn,
                storage=storage,
                metadata_schema=target.meta_schema,
            ),
            override_data_path=True,
        ),
        dataset_name=SOURCE_SCHEMA,
    )
