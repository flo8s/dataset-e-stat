"""DuckLake カタログ破損診断 (read-only)。

dbt build が全モデルで失敗する
    Invalid Input Error: Corrupt DuckLake - multiple snapshots returned from database
の原因を、Neon Postgres 上の ducklake_* メタデータを **ducklake 拡張を介さず**
素の Postgres として読み取って調査する。ducklake 拡張で ATTACH すると破損判定で
落ちるため、snapshot-to-r2.py と同じ TYPE POSTGRES READ_ONLY 接続で生メタデータを見る。

すべて読み取り専用。データやメタデータは一切変更しない。

Usage:
    NEON_DATABASE_URL=postgresql://... python scripts/diagnose_ducklake.py [meta_schema]
    # meta_schema 既定: e_stat (本番)。dev は dev_e_stat
"""

from __future__ import annotations

import os
import sys

import duckdb


def main() -> None:
    dsn = os.environ.get("NEON_DATABASE_URL")
    if not dsn:
        raise SystemExit("環境変数 NEON_DATABASE_URL が必要です")
    schema = sys.argv[1] if len(sys.argv) > 1 else "e_stat"

    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL postgres; LOAD postgres;")
    conn.execute(f"ATTACH '{dsn}' AS pg (TYPE POSTGRES, READ_ONLY)")

    def q(sql: str, params: list | None = None):
        return conn.execute(sql, params or []).fetchall()

    def show(title: str, sql: str, params: list | None = None) -> None:
        print(f"\n=== {title} ===")
        try:
            rows = q(sql, params)
            if not rows:
                print("  (no rows)")
            for r in rows:
                print("  " + " | ".join("" if v is None else str(v) for v in r))
        except Exception as e:  # noqa: BLE001  診断なので握りつぶして続行
            print(f"  ERROR: {e}")

    # 0. ducklake_* テーブル一覧 + 行数
    tables = [
        r[0]
        for r in q(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog='pg' AND table_schema=? "
            "AND table_name LIKE 'ducklake_%' ORDER BY 1",
            [schema],
        )
    ]
    print(f"schema={schema!r}: {len(tables)} 個の ducklake_* テーブル")
    for t in tables:
        try:
            n = q(f'SELECT count(*) FROM pg.{schema}."{t}"')[0][0]
            print(f"  {t}: {n} rows")
        except Exception as e:  # noqa: BLE001
            print(f"  {t}: ERROR {e}")

    if not tables:
        raise SystemExit(
            f"schema {schema!r} に ducklake_* テーブルが見つかりません。schema 名を確認してください。"
        )

    # 参考: 主要テーブルの実カラム名 (以降のクエリのカラム名が環境と違う場合の手掛かり)
    for t in ("ducklake_snapshot", "ducklake_metadata", "ducklake_schema", "ducklake_table"):
        if t in tables:
            show(
                f"{t} カラム一覧",
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_catalog='pg' AND table_schema=? AND table_name=? "
                "ORDER BY ordinal_position",
                [schema, t],
            )

    # 1. ducklake_metadata: 全ダンプ + 重複キー
    #    同じ key が複数あると「version 等の単一値取得」で multiple になり得る
    show(
        "ducklake_metadata (key/value 全件)",
        f"SELECT * FROM pg.{schema}.ducklake_metadata ORDER BY 1",
    )
    show(
        "ducklake_metadata 重複キー (★ multiple の候補)",
        f"SELECT key, count(*) AS c FROM pg.{schema}.ducklake_metadata "
        f"GROUP BY key HAVING count(*) > 1 ORDER BY c DESC",
    )

    # 2. ducklake_snapshot: 概況 + 重複 snapshot_id
    show(
        "ducklake_snapshot 概況 (件数 / min,max snapshot_id)",
        f"SELECT count(*) AS n, min(snapshot_id) AS min_id, max(snapshot_id) AS max_id "
        f"FROM pg.{schema}.ducklake_snapshot",
    )
    show(
        "ducklake_snapshot 重複 snapshot_id (★ multiple snapshots の最有力原因)",
        f"SELECT snapshot_id, count(*) AS c FROM pg.{schema}.ducklake_snapshot "
        f"GROUP BY snapshot_id HAVING count(*) > 1 ORDER BY c DESC",
    )
    show(
        "ducklake_snapshot 直近10件",
        f"SELECT * FROM pg.{schema}.ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 10",
    )
    show(
        "schema_version 分布",
        f"SELECT schema_version, count(*) AS c FROM pg.{schema}.ducklake_snapshot "
        f"GROUP BY schema_version ORDER BY 1",
    )

    # 3. 「現在有効」エントリの重複: 同名で end_snapshot IS NULL が複数あると
    #    そのテーブル/スキーマに対し複数版が有効 = 読み取りで multiple になり得る
    for tbl, namecol in (("ducklake_schema", "schema_name"), ("ducklake_table", "table_name")):
        if tbl in tables:
            show(
                f"{tbl}: 同一 {namecol} で end_snapshot IS NULL が複数 (★ 有効版の重複)",
                f'SELECT {namecol}, count(*) AS c FROM pg.{schema}."{tbl}" '
                f"WHERE end_snapshot IS NULL GROUP BY {namecol} HAVING count(*) > 1 ORDER BY c DESC",
            )

    conn.close()
    print("\n--- 診断完了 (read-only。一切変更していません) ---")


if __name__ == "__main__":
    main()
