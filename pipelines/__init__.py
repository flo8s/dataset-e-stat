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
from queria_config import load_target, require_motherduck_token  # noqa: E402


def create_pipeline():
    """dlt パイプラインを MotherDuck DuckLake + R2 (BYOB) で構成する。

    - catalog: MotherDuck の内部 metadata DB (md:__ducklake_metadata_<db>)
    - storage: R2 バケット (s3:// プロトコル + R2 endpoint で接続)
    """
    target_name = os.environ.get("DBT_TARGET", "default")
    target = load_target(target_name)
    token = require_motherduck_token()
    # dlt が motherduck 拡張から token を読み取れるよう環境変数で公開
    os.environ.setdefault("MOTHERDUCK_TOKEN", token)

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
                catalog=f"md:__ducklake_metadata_{target.motherduck_db}",
                storage=storage,
            ),
            override_data_path=True,
        ),
        dataset_name=SOURCE_SCHEMA,
    )
