"""パイプライン共通セットアップ。"""

import logging
from enum import IntEnum

import dlt
from fdl.ducklake import create_destination


class EstatStatus(IntEnum):
    """e-Stat API レスポンスステータスコード。

    https://www.e-stat.go.jp/api/api-info/e-stat-manual3-0#sec3
    """

    OK = 0  # 正常終了
    NO_DATA = 1  # 正常終了（該当データなし）
    PARTIAL = 2  # 正常終了（条件不一致、部分的な結果）
    # 100+ はエラー

# dlt の ArrowExtractor が merge 時に出す column hints 差異の WARNING を抑制
logging.getLogger("dlt.extract.extractors").setLevel(logging.ERROR)

SOURCE_SCHEMA = "_source"


def create_pipeline():
    """共通の dlt パイプラインを作成する。"""
    destination = create_destination()
    return dlt.pipeline(
        pipeline_name="estat",
        destination=destination,
        dataset_name=SOURCE_SCHEMA,
    )
