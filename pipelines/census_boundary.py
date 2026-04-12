"""国勢調査 町丁・字等別境界データのダウンロード。

e-Stat 統計GIS から都道府県別の GML をダウンロードし展開する。
GML は UTF-8 ネイティブなので Shapefile の CP932 エンコーディング問題が発生しない。

データソース: 令和2年国勢調査 町丁・字等別境界データ（世界測地系緯度経度・GML）
https://www.e-stat.go.jp/gis/statmap-search?page=1&type=2&aggregateUnitForBoundary=A&toukeiCode=00200521&toukeiYear=2020&serveyId=A002005212020&coordsys=1&format=gml&datum=2011
"""

import logging
import time
import zipfile
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

logger = logging.getLogger("pipelines")

BASE_URL = (
    "https://www.e-stat.go.jp/gis/statmap-search/data"
    "?dlserveyId=A002005212020"
    "&code={code}"
    "&coordSys=1&format=gml&downloadType=5&datum=2011"
)

# fmt: off
PREFECTURES = [
    {"code": "01", "gml": "r2ka01.gml"},
    {"code": "02", "gml": "r2ka02.gml"},
    {"code": "03", "gml": "r2ka03.gml"},
    {"code": "04", "gml": "r2ka04.gml"},
    {"code": "05", "gml": "r2ka05.gml"},
    {"code": "06", "gml": "r2ka06.gml"},
    {"code": "07", "gml": "r2ka07.gml"},
    {"code": "08", "gml": "r2ka08.gml"},
    {"code": "09", "gml": "r2ka09.gml"},
    {"code": "10", "gml": "r2ka10.gml"},
    {"code": "11", "gml": "r2ka11.gml"},
    {"code": "12", "gml": "r2ka12.gml"},
    {"code": "13", "gml": "r2ka13.gml"},
    {"code": "14", "gml": "r2ka14.gml"},
    {"code": "15", "gml": "r2ka15.gml"},
    {"code": "16", "gml": "r2ka16.gml"},
    {"code": "17", "gml": "r2ka17.gml"},
    {"code": "18", "gml": "r2ka18.gml"},
    {"code": "19", "gml": "r2ka19.gml"},
    {"code": "20", "gml": "r2ka20.gml"},
    {"code": "21", "gml": "r2ka21.gml"},
    {"code": "22", "gml": "r2ka22.gml"},
    {"code": "23", "gml": "r2ka23.gml"},
    {"code": "24", "gml": "r2ka24.gml"},
    {"code": "25", "gml": "r2ka25.gml"},
    {"code": "26", "gml": "r2ka26.gml"},
    {"code": "27", "gml": "r2ka27.gml"},
    {"code": "28", "gml": "r2ka28.gml"},
    {"code": "29", "gml": "r2ka29.gml"},
    {"code": "30", "gml": "r2ka30.gml"},
    {"code": "31", "gml": "r2ka31.gml"},
    {"code": "32", "gml": "r2ka32.gml"},
    {"code": "33", "gml": "r2ka33.gml"},
    {"code": "34", "gml": "r2ka34.gml"},
    {"code": "35", "gml": "r2ka35.gml"},
    {"code": "36", "gml": "r2ka36.gml"},
    {"code": "37", "gml": "r2ka37.gml"},
    {"code": "38", "gml": "r2ka38.gml"},
    {"code": "39", "gml": "r2ka39.gml"},
    {"code": "40", "gml": "r2ka40.gml"},
    {"code": "41", "gml": "r2ka41.gml"},
    {"code": "42", "gml": "r2ka42.gml"},
    {"code": "43", "gml": "r2ka43.gml"},
    {"code": "44", "gml": "r2ka44.gml"},
    {"code": "45", "gml": "r2ka45.gml"},
    {"code": "46", "gml": "r2ka46.gml"},
    {"code": "47", "gml": "r2ka47.gml"},
]
# fmt: on

_TRANSIENT_HTTP_CODES = {502, 503, 504}
_MAX_RETRIES = 4
_TIMEOUT = 60  # seconds


def _download_with_retry(req: Request, dest: Path) -> None:
    """Download a URL to a file with retry on transient errors."""
    for attempt in range(_MAX_RETRIES):
        try:
            with urlopen(req, timeout=_TIMEOUT) as resp, open(dest, "wb") as f:
                f.write(resp.read())
            return
        except HTTPError as e:
            if e.code not in _TRANSIENT_HTTP_CODES or attempt == _MAX_RETRIES - 1:
                raise
            wait = 2 ** attempt
            logger.warning(f"  HTTP {e.code}, retry in {wait}s ({attempt + 1}/{_MAX_RETRIES})")
            time.sleep(wait)
        except URLError as e:
            if attempt == _MAX_RETRIES - 1:
                raise
            wait = 2 ** attempt
            logger.warning(f"  {e.reason}, retry in {wait}s ({attempt + 1}/{_MAX_RETRIES})")
            time.sleep(wait)


def download_boundary(dest_dir: str) -> None:
    """全都道府県の境界 GML をダウンロードし展開する。

    既にダウンロード済みの都道府県はスキップする。
    """
    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)

    for pref in PREFECTURES:
        gml_path = dest / pref["gml"]

        if gml_path.exists():
            logger.info(f"  skip {pref['code']} (already exists)")
            continue

        url = BASE_URL.format(code=pref["code"])
        zip_path = dest / f"{pref['code']}.zip"

        logger.info(f"  downloading {pref['code']}...")
        req = Request(url, headers={"User-Agent": "dataset-e-stat"})
        _download_with_retry(req, zip_path)

        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(dest)

        zip_path.unlink()

    logger.info(f"  {len(PREFECTURES)} prefectures ready in {dest}")
