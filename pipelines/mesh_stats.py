"""地域メッシュ統計（1kmメッシュ）の統計値ダウンロード。

e-Stat 統計GIS から都道府県別の CSV をダウンロードし、UTF-8 に変換して展開する。
境界データと違い API キーは不要で、statsId と都道府県コードだけで取得できる。

配布 CSV には次の 2 つの癖があるため、ここで正規化してから dbt に渡す。

1. 文字コードが CP932。DuckDB の read_csv は CP932 を読めないので UTF-8 に変換する。
2. ヘッダーが 2 行ある。1 行目が列コード (KEY_CODE, T001179001, ...)、2 行目が
   日本語ラベル、3 行目以降がデータ。2 行目を捨てて通常の 1 行ヘッダーにする。

データソース: 令和2年国勢調査・令和3年経済センサス‐活動調査に関する地域メッシュ統計
https://www.e-stat.go.jp/gis
"""

import logging
import zipfile
from pathlib import Path
from urllib.request import Request

from pipelines.gis_download import download_with_retry

logger = logging.getLogger("pipelines")

BASE_URL = (
    "https://www.e-stat.go.jp/gis/statmap-search/data"
    "?statsId={stats_id}"
    "&code={code}"
    "&downloadType=2"
)

# 配布単位である都道府県コード。
PREFECTURE_CODES = [f"{i:02d}" for i in range(1, 48)]

# 取り込む統計表。集計単位サフィックス S は基準地域メッシュ（1kmメッシュ）。
#
# 同じ内容の表が調査年・測地系ごとに別 statsId で並んでいるため、都道府県合計を
# 公表値と突合して次のとおり同定した（東京都 = code 13 で実測）。
#   - T001171/T001179/T001181: 令和2年国勢調査。人口総数 14,047,594、
#     労働力人口 6,187,583 が公表値と一致する。
#     平成27年の同型表は T001176/T001200/T001202（13,515,271 / 6,094,436）。
#     令和2年の別測地系は T001188/T001189/T001191。
#   - T001157: 令和3年経済センサス‐活動調査。編成項目に「Ａ～Ｓ全産業」と
#     「Ｆ～Ｓ第３次産業」があるのが令和3年の特徴で、平成28年（T001209）は
#     「Ａ～Ｒ全産業（Ｓ公務を除く）」「Ｆ～Ｒ第３次産業」しか持たない。
#     https://www.stat.go.jp/data/mesh/pdf/r3keisen.pdf
#
# 測地系は JGD2000 系を選んでいる。既存の boundary.mesh_1km が EPSG:4612 = JGD2000
# であることに合わせたもの。JGD2000 と JGD2011 のメッシュ区画のずれは国内で
# 数十cm〜数m のため、どちらでもメッシュコードでの結合結果は変わらない。
MESH_STATS_TABLES = [
    {
        "name": "mesh_population",
        "stats_id": "T001171",
        "description": "令和2年国勢調査 年齢（5歳階級）別、男女別人口",
    },
    {
        "name": "mesh_labor",
        "stats_id": "T001179",
        "description": "令和2年国勢調査 労働力状態別人口、産業別就業者数",
    },
    {
        "name": "mesh_commute",
        "stats_id": "T001181",
        "description": "令和2年国勢調査 従業地・通学地別人口、在学者数、未就学者数",
    },
    {
        "name": "mesh_establishment",
        "stats_id": "T001157",
        "description": "令和3年経済センサス‐活動調査 産業別事業所数、従業者数",
    },
]


def csv_name(stats_id: str, code: str) -> str:
    """変換後の CSV ファイル名。dbt の raw モデルが glob で読む。"""
    return f"{stats_id}_{code}.csv"


def _normalize(zip_path: Path, csv_path: Path) -> None:
    """zip 内の CP932 / 2行ヘッダーの CSV を UTF-8 / 1行ヘッダーに変換する。"""
    with zipfile.ZipFile(zip_path) as zf:
        members = [n for n in zf.namelist() if n.lower().endswith(".txt")]
        if len(members) != 1:
            raise ValueError(f"expected exactly one .txt in {zip_path.name}: {members}")
        raw = zf.read(members[0]).decode("cp932")

    lines = raw.replace("\r\n", "\n").split("\n")
    if len(lines) < 2:
        raise ValueError(f"{zip_path.name} has no data rows")

    # 2 行目（日本語ラベル行）を捨てる。ラベルは列コードから引けるので不要。
    csv_path.write_text("\n".join([lines[0]] + lines[2:]), encoding="utf-8")


def download_mesh_stats(dest_dir: str) -> None:
    """全統計表 × 全都道府県のメッシュ統計 CSV をダウンロードし変換する。

    既に変換済みの CSV はスキップする。
    """
    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)

    for spec in MESH_STATS_TABLES:
        stats_id = spec["stats_id"]
        downloaded = 0

        for code in PREFECTURE_CODES:
            csv_path = dest / csv_name(stats_id, code)

            if csv_path.exists():
                continue

            url = BASE_URL.format(stats_id=stats_id, code=code)
            zip_path = dest / f"{stats_id}_{code}.zip"
            download_with_retry(Request(url), zip_path)
            _normalize(zip_path, csv_path)
            zip_path.unlink()
            downloaded += 1

        logger.info(
            f"  {spec['name']} ({stats_id}): {downloaded} downloaded, "
            f"{len(PREFECTURE_CODES) - downloaded} cached"
        )
