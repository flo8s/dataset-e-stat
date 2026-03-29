"""Upload dbt artifacts to S3 for catalog consumption."""

from __future__ import annotations

import os
import tomllib
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
TARGET_DIR = PROJECT_DIR / "target"
FDL_TOML = PROJECT_DIR / "fdl.toml"

ARTIFACTS = ["manifest.json", "catalog.json"]


def main() -> None:
    import boto3

    with open(FDL_TOML, "rb") as f:
        config = tomllib.load(f)
    datasource = config["name"]

    bucket = os.environ["FDL_S3_BUCKET"]
    client = boto3.client(
        "s3",
        endpoint_url=os.environ["FDL_S3_ENDPOINT"],
        aws_access_key_id=os.environ["FDL_S3_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["FDL_S3_SECRET_ACCESS_KEY"],
    )

    for name in ARTIFACTS:
        src = TARGET_DIR / name
        if not src.exists():
            print(f"  {name}: not found, skipping")
            continue
        key = f"{datasource}/dbt/{name}"
        client.upload_file(
            str(src),
            bucket,
            key,
            ExtraArgs={"ContentType": "application/json; charset=utf-8"},
        )
        print(f"  {key}")

    print(f"Uploaded artifacts for {datasource}")


if __name__ == "__main__":
    main()
