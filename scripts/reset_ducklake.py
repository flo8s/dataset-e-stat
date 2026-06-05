"""DuckLake カタログ作り直し (DESTRUCTIVE / 破壊的)。

破損した DuckLake カタログ (重複 snapshot 等) を初期化する:
  1. Neon: メタデータスキーマ (既定 e_stat) を DROP & 空で再作成  [必須・本質的な修復]
  2. R2 : <dataset>/ducklake.duckdb.files/ 配下の parquet を削除   [任意・掃除]

実データは e-Stat API が真のソースのため、次回 sync 実行でカタログ・データとも
再構築される。R2 の旧ファイルは新カタログから参照されないので残っても無害
(掃除目的でのみ削除)。

安全装置: 環境変数 CONFIRM が 'RESET <meta_schema>' と完全一致しないと何もしない。
すべて明示確認が必要な破壊的操作。

Usage (CI):
    CONFIRM='RESET e_stat' NEON_DATABASE_URL=... [QUERIA_S3_*...] \
        python scripts/reset_ducklake.py [meta_schema]
"""

from __future__ import annotations

import os
import re
import sys
import tomllib
from pathlib import Path

import duckdb


def dataset_name() -> str:
    """pyproject.toml [tool.queria].name (= R2 のデータセットプレフィックス)。"""
    with open(Path(__file__).resolve().parent.parent / "pyproject.toml", "rb") as f:
        return tomllib.load(f)["tool"]["queria"]["name"]


def main() -> None:
    meta_schema = sys.argv[1] if len(sys.argv) > 1 else "e_stat"
    if not re.fullmatch(r"[A-Za-z0-9_]+", meta_schema):
        raise SystemExit(f"不正な meta_schema: {meta_schema!r}")

    confirm = os.environ.get("CONFIRM", "")
    expected = f"RESET {meta_schema}"
    if confirm != expected:
        raise SystemExit(
            f"確認文字列が一致しません。CONFIRM='{expected}' を設定してください (現在: {confirm!r})"
        )

    dsn = os.environ.get("NEON_DATABASE_URL")
    if not dsn:
        raise SystemExit("環境変数 NEON_DATABASE_URL が必要です")

    # 1. Neon メタデータスキーマを初期化 (必須)。
    #    postgres_execute で素の Postgres DDL を実行 (ducklake 拡張は使わない)。
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL postgres; LOAD postgres;")
    conn.execute(f"ATTACH '{dsn}' AS pg (TYPE POSTGRES)")
    for ddl in (
        f'DROP SCHEMA IF EXISTS "{meta_schema}" CASCADE',
        f'CREATE SCHEMA "{meta_schema}"',
    ):
        conn.execute("CALL postgres_execute('pg', '" + ddl.replace("'", "''") + "')")
    conn.close()
    print(f"[Neon] schema {meta_schema!r} を DROP し空で再作成しました")

    # 2. R2 データファイル削除 (任意・掃除。S3 認証情報が無ければスキップ)。
    endpoint = os.environ.get("QUERIA_S3_ENDPOINT")
    bucket = os.environ.get("QUERIA_S3_BUCKET")
    key_id = os.environ.get("QUERIA_S3_ACCESS_KEY_ID")
    secret = os.environ.get("QUERIA_S3_SECRET_ACCESS_KEY")
    if all([endpoint, bucket, key_id, secret]):
        import boto3

        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=key_id,
            aws_secret_access_key=secret,
        )
        prefix = f"{dataset_name()}/ducklake.duckdb.files/"
        deleted = 0
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=prefix
        ):
            objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if objs:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": objs})
                deleted += len(objs)
        print(f"[R2] {prefix!r} 配下の {deleted} オブジェクトを削除しました")
    else:
        print("[R2] S3 認証情報が無いため削除はスキップ (孤立ファイルは無害。次回 sync で新規作成)")

    print("完了。次回の sync 実行でカタログとデータが再構築されます。")


if __name__ == "__main__":
    main()
