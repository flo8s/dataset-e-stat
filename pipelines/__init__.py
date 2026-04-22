"""パイプライン共通セットアップ。"""

import logging
import logging.config
import os
from enum import IntEnum

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


def create_pipeline():
    """共通の dlt パイプラインを作成する。"""
    catalog_url = os.environ["FDL_CATALOG_URL"]
    data_url = os.environ["FDL_DATA_URL"]

    storage = data_url
    if data_url.startswith("s3://"):
        from dlt.common.configuration.specs import AwsCredentials
        from dlt.common.storages.configuration import FilesystemConfiguration

        storage = FilesystemConfiguration(
            bucket_url=data_url,
            credentials=AwsCredentials(
                aws_access_key_id=os.environ["FDL_S3_ACCESS_KEY_ID"],
                aws_secret_access_key=os.environ["FDL_S3_SECRET_ACCESS_KEY"],
                endpoint_url=os.environ.get("FDL_S3_ENDPOINT"),
                region_name="auto",
                s3_url_style="path",
            ),
        )

    return dlt.pipeline(
        pipeline_name="estat",
        destination=ducklake(
            credentials=DuckLakeCredentials(
                catalog=catalog_url,
                storage=storage,
            ),
            override_data_path=True,
        ),
        dataset_name=SOURCE_SCHEMA,
    )
