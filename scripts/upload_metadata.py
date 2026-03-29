"""Generate metadata.json from dbt artifacts + fdl.toml and upload to S3."""

from __future__ import annotations

import json
import os
import tomllib
from collections import defaultdict
from pathlib import Path

from dbt.artifacts.resources.v1.model import Model
from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.artifacts.schemas.manifest import WritableManifest

PROJECT_DIR = Path(__file__).resolve().parent.parent
TARGET_DIR = PROJECT_DIR / "target"
FDL_TOML = PROJECT_DIR / "fdl.toml"
OUTPUT_PATH = PROJECT_DIR / ".fdl" / "metadata.json"


def load_config() -> dict:
    with open(FDL_TOML, "rb") as f:
        return tomllib.load(f)


def load_manifest() -> WritableManifest:
    return WritableManifest.read_and_check_versions(str(TARGET_DIR / "manifest.json"))


def load_catalog() -> CatalogArtifact | None:
    path = TARGET_DIR / "catalog.json"
    if not path.exists():
        return None
    return CatalogArtifact.read_and_check_versions(str(path))


def resolve_column_type(
    col_name: str, col_info_data_type: str | None, catalog_columns: dict
) -> str:
    if col_name in catalog_columns:
        catalog_type = catalog_columns[col_name].type
        if catalog_type:
            return catalog_type
    return col_info_data_type or ""


def build_columns(node: Model, catalog_columns: dict) -> list[dict]:
    return [
        {
            "name": col_name,
            "title": col_info.meta.get("title", ""),
            "description": col_info.description,
            "data_type": resolve_column_type(col_name, col_info.data_type, catalog_columns),
            "nullable": not any(c.type.value == "not_null" for c in col_info.constraints),
        }
        for col_name, col_info in node.columns.items()
    ]


def build_model_info(node: Model, catalog_columns: dict) -> dict:
    meta = node.meta
    return {
        "name": node.name,
        "title": meta.get("title", ""),
        "description": node.description,
        "tags": meta.get("tags", []),
        "license": meta.get("license", ""),
        "license_url": meta.get("license_url", ""),
        "source_url": meta.get("source_url", ""),
        "published": meta.get("published", False),
        "materialized": node.config.materialized,
        "columns": build_columns(node, catalog_columns),
        "sql": (node.compiled_code or "").strip() or None,
        "file_path": node.original_file_path or "",
    }


def extract_models(
    manifest: WritableManifest, catalog: CatalogArtifact | None, datasource: str
) -> dict[str, list[dict]]:
    tables_by_schema: dict[str, list[dict]] = defaultdict(list)
    for node_id, node in manifest.nodes.items():
        if not isinstance(node, Model):
            continue
        if not node.fqn or node.fqn[0] != datasource:
            continue
        catalog_node = catalog.nodes.get(node_id) if catalog else None
        catalog_columns = catalog_node.columns if catalog_node else {}
        tables_by_schema[node.schema].append(build_model_info(node, catalog_columns))
    return dict(tables_by_schema)


def extract_lineage(manifest: WritableManifest, datasource: str) -> dict:
    prefix = f"model.{datasource}."
    parent_map_raw = manifest.parent_map or {}

    parent_map: dict[str, list[str]] = {}
    node_keys: set[str] = set()

    for full_key, parents in parent_map_raw.items():
        if not full_key.startswith(prefix):
            continue
        short_key = full_key[len(prefix):]
        short_parents = [p[len(prefix):] for p in parents if p.startswith(prefix)]
        parent_map[short_key] = short_parents
        node_keys.add(short_key)
        node_keys.update(short_parents)

    nodes: dict[str, dict] = {}
    for key in node_keys:
        full_key = prefix + key
        node = manifest.nodes.get(full_key)
        if node:
            nodes[key] = {
                "fqn": node.fqn,
                "resource_type": node.resource_type,
                "config": {"materialized": node.config.materialized},
                "meta": node.meta,
            }
        else:
            nodes[key] = {
                "fqn": [],
                "resource_type": "model",
                "config": {"materialized": "view"},
                "meta": {},
            }

    return {"parent_map": parent_map, "nodes": nodes}


def generate_metadata(config: dict, datasource: str) -> dict:
    meta = config.get("meta", {})
    target = config.get("targets", {}).get("default", {})
    public_url = os.path.expandvars(target.get("public_url", ""))
    ducklake_url = f"{public_url}/{datasource}/ducklake.duckdb"

    manifest = load_manifest()
    catalog = load_catalog()
    tables_by_schema = extract_models(manifest, catalog, datasource)
    lineage = extract_lineage(manifest, datasource)

    schemas = {}
    for name, schema_config in meta.get("schemas", {}).items():
        schemas[name] = {
            "title": schema_config.get("title", ""),
            "tables": tables_by_schema.get(name, []),
        }
    for name, tables in tables_by_schema.items():
        if name not in schemas:
            schemas[name] = {"title": "", "tables": tables}

    readme_path = PROJECT_DIR / "README.md"
    readme = readme_path.read_text() if readme_path.exists() else None

    result = {
        "title": meta.get("title", ""),
        "description": meta.get("description", ""),
        "cover": meta.get("cover", ""),
        "tags": meta.get("tags", []),
        "ducklake_url": ducklake_url,
        "repository_url": meta.get("repository_url", ""),
        "schemas": schemas,
        "lineage": lineage,
    }
    if readme:
        result["readme"] = readme
    return result


def upload_to_s3(datasource: str) -> None:
    import boto3

    bucket = os.environ["FDL_S3_BUCKET"]
    endpoint = os.environ["FDL_S3_ENDPOINT"]
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ["FDL_S3_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["FDL_S3_SECRET_ACCESS_KEY"],
    )
    key = f"{datasource}/metadata.json"
    client.upload_file(
        str(OUTPUT_PATH),
        bucket,
        key,
        ExtraArgs={"ContentType": "application/json; charset=utf-8"},
    )
    print(f"  Uploaded: s3://{bucket}/{key}")


def main() -> None:
    config = load_config()
    datasource = config["name"]

    metadata = generate_metadata(config, datasource)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

    total_tables = sum(len(s["tables"]) for s in metadata["schemas"].values())
    print(f"Generated: {OUTPUT_PATH}")
    print(f"  Datasource: {datasource} / Tables: {total_tables}")

    upload_to_s3(datasource)


if __name__ == "__main__":
    main()
