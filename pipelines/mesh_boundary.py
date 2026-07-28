"""標準地域メッシュ（3次メッシュ＝1kmメッシュ）境界データのダウンロード。

e-Stat 統計GIS から 1次メッシュ（80kmメッシュ）単位の GML をダウンロードし展開する。
GML は UTF-8 ネイティブなので Shapefile の CP932 エンコーディング問題が発生しない。

データソース: 3次メッシュ（1kmメッシュ）境界データ（世界測地系緯度経度・GML）
https://www.e-stat.go.jp/gis/statmap-search?page=1&type=2&aggregateUnitForBoundary=S&coordsys=1&format=gml
"""

import logging
import zipfile
from pathlib import Path
from urllib.request import Request

from pipelines.gis_download import download_with_retry

logger = logging.getLogger("pipelines")

BASE_URL = (
    "https://www.e-stat.go.jp/gis/statmap-search/data"
    "?dlserveyId=S"
    "&code={code}"
    "&coordSys=1&format=gml&downloadType=5"
)

# 配布単位である 1次メッシュのコード。標準地域メッシュ（JIS X 0410）の区画は
# 固定なので、この一覧も固定（統計GIS の配布一覧に載る 176 区画）。
# fmt: off
MESH1_CODES = [
    "3036", "3622", "3623", "3624", "3631", "3641", "3653", "3724", "3725", "3741", "3823", "3824",
    "3831", "3841", "3926", "3927", "3928", "3942", "4027", "4028", "4040", "4042", "4128", "4129",
    "4142", "4229", "4230", "4328", "4329", "4429", "4440", "4529", "4530", "4531", "4540", "4629",
    "4630", "4631", "4728", "4729", "4730", "4731", "4739", "4740", "4828", "4829", "4830", "4831",
    "4839", "4928", "4929", "4930", "4931", "4932", "4933", "4934", "4939", "5029", "5030", "5031",
    "5032", "5033", "5034", "5035", "5036", "5038", "5039", "5129", "5130", "5131", "5132", "5133",
    "5134", "5135", "5136", "5137", "5138", "5139", "5229", "5231", "5232", "5233", "5234", "5235",
    "5236", "5237", "5238", "5239", "5240", "5332", "5333", "5334", "5335", "5336", "5337", "5338",
    "5339", "5340", "5432", "5433", "5435", "5436", "5437", "5438", "5439", "5440", "5531", "5536",
    "5537", "5538", "5539", "5540", "5541", "5636", "5637", "5638", "5639", "5640", "5641", "5738",
    "5739", "5740", "5741", "5839", "5840", "5841", "5939", "5940", "5941", "5942", "6039", "6040",
    "6041", "6139", "6140", "6141", "6239", "6240", "6241", "6243", "6339", "6340", "6341", "6342",
    "6343", "6439", "6440", "6441", "6442", "6443", "6444", "6445", "6540", "6541", "6542", "6543",
    "6544", "6545", "6546", "6641", "6642", "6643", "6644", "6645", "6646", "6647", "6740", "6741",
    "6742", "6747", "6748", "6840", "6841", "6842", "6847", "6848",
]
# fmt: on


def gml_name(mesh1_code: str) -> str:
    """1次メッシュコードに対応する GML ファイル名。"""
    return f"MESH0{mesh1_code}.gml"


def download_mesh_boundary(dest_dir: str) -> None:
    """全 1次メッシュの 1kmメッシュ境界 GML をダウンロードし展開する。

    既にダウンロード済みの 1次メッシュはスキップする。
    """
    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)

    for code in MESH1_CODES:
        gml_path = dest / gml_name(code)

        if gml_path.exists():
            continue

        url = BASE_URL.format(code=code)
        zip_path = dest / f"{code}.zip"

        logger.info(f"  downloading {code}...")
        req = Request(url, headers={"User-Agent": "dataset-e-stat"})
        download_with_retry(req, zip_path)

        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(dest)

        zip_path.unlink()

    logger.info(f"  {len(MESH1_CODES)} mesh1 blocks ready in {dest}")
