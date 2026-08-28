"""経済センサス‐活動調査 産業横断的集計の取得 (getStatsData)。

国勢調査の市区町村別集計 (census_municipality) と同じく searchKind=1 の通常の
統計表なので、統計表 ID は政府統計コード・集計の名前・統計表番号 (TITLE の @no)
で引ける。

■ 「売上（収入）金額等」の表を取る理由

事業所数と従業者数だけなら「事業所数、従業者数」の集計にも市区町村別の表がある
(第6-1表など)。売上（収入）金額まで市区町村別に載るのは「売上（収入）金額等」の
第2-1表だけで、事業所数・従業者数もこの表に同じ集計対象で入っている。3指標を
1つの表から取れば、集計対象の違う数字が同じ行に並ぶことがない。

■ 事業所数がほかの表と一致しない

この表の事業所数は 4,870,898 で、事業所に関する集計の民営事業所数 5,156,063
(第6-1表・全国・全産業) と一致しない。売上（収入）金額等の集計は必要な事項の
数値が得られた事業所だけを対象にするため、母数が約 5.5% 小さい。他表の事業所数と
突き合わせる用途には使えない。

■ merge キーに time を含める

この統計表の時間軸は 2021年 の 1 値だけだが、primary_key には time も入れる。
外すと、e-Stat がこの表に別時点を足したときに time 以外が同じ行どうしが 1 行に
マージされ、行数も恒等式も崩れないまま後勝ちでデータが消える。

■ 売上（収入）金額が無い産業

売上（収入）金額は産業によって調査されておらず、建設業・電気ガス・運輸業・
金融保険業などは「･･･」(調査していないもの) で入る。内訳の一部が欠ける産業は
その大分類の合計も欠けるため、全産業 (AR) と非農林漁業 (CR) の売上も無い。
事業所数・従業者数は全産業で揃っている。
"""

import logging
from typing import Any, Dict, List

from estat_api_dlt_helper import estat_source, estat_table
from estat_api_dlt_helper.api.client import EstatApiClient

from pipelines import EstatStatus
from pipelines.ssds import drop_stat_inf

logger = logging.getLogger(__name__)

# 経済センサス‐活動調査 (政府統計コード)
ECONOMIC_CENSUS_STATS_CODE = "00200553"
# 令和3年経済センサス‐活動調査 (2021年6月1日現在)
SURVEY_YEAR = 2021

# 取り込む統計表。table_no は TITLE の @no、tabulation は STATISTICS_NAME に
# 含まれる集計の名前。
ECONOMIC_CENSUS_TABLES: List[Dict[str, Any]] = [
    {
        "name": "economic_census_industry",
        "tabulation": "産業横断的集計 売上（収入）金額等",
        "table_no": "2-1",
        "primary_key": ["tab", "cat01", "cat02", "area", "time"],
    },
]


def fetch_economic_census_ids(app_id: str, specs: List[Dict[str, Any]]) -> Dict[str, str]:
    """統計表番号と集計の種類から statsDataId を引く。

    Args:
        app_id: e-Stat API アプリケーション ID。
        specs: ECONOMIC_CENSUS_TABLES の要素。table_no と tabulation を見る。

    Returns:
        テーブル名 -> statsDataId の対応。

    Raises:
        RuntimeError: API がエラーを返したとき、および統計表番号が 1 件に
            定まらなかったとき。0 件を握り潰すと、e-Stat 側の表番号が変わっても
            dbt は前回ロード済みの _source でビルドに成功し、CI が緑のまま
            テーブルが更新されなくなる。
    """
    client = EstatApiClient(app_id=app_id, timeout=300)
    result = client.get_stats_list(
        statsCode=ECONOMIC_CENSUS_STATS_CODE, surveyYears=SURVEY_YEAR, limit=100000
    )

    stats_list = result.get("GET_STATS_LIST", {})
    status = stats_list.get("RESULT", {}).get("STATUS")
    if status not in (EstatStatus.OK, EstatStatus.PARTIAL):
        error_msg = stats_list.get("RESULT", {}).get("ERROR_MSG", "Unknown error")
        raise RuntimeError(f"economic_census: API error (status {status}): {error_msg}")

    tables = stats_list.get("DATALIST_INF", {}).get("TABLE_INF", [])
    if isinstance(tables, dict):
        tables = [tables]

    resolved: Dict[str, str] = {}
    for spec in specs:
        no = spec["table_no"]
        tabulation = spec["tabulation"]
        # 同じ @id が複数回現れるため集合で扱う。
        ids = {
            t["@id"]
            for t in tables
            if isinstance(t.get("TITLE"), dict)
            and t["TITLE"].get("@no") == no
            and tabulation in str(t.get("STATISTICS_NAME") or "")
        }
        if len(ids) != 1:
            raise RuntimeError(
                f"economic_census: table_no={no} tabulation={tabulation} "
                f"matched {len(ids)} ids ({sorted(ids)}) in "
                f"statsCode={ECONOMIC_CENSUS_STATS_CODE} surveyYears={SURVEY_YEAR} "
                f"({len(tables)} rows scanned). "
                "e-Stat 側の表番号が変わった可能性がある。"
                "ECONOMIC_CENSUS_TABLES を確認する。"
            )
        resolved[spec["name"]] = ids.pop()

    logger.info(f"economic_census: resolved {resolved}")
    return resolved


def create_economic_census_source(app_id: str, ids: Dict[str, str]):
    """産業横断的集計のソースを作成する。

    stat_inf は SSDS・census 系と同様に除去する。統計表単位のメタ情報が全行に
    複製される冗長な列で、stg / mart からは参照しない。
    """
    resources = []
    for spec in ECONOMIC_CENSUS_TABLES:
        resource = estat_table(
            stats_data_id=ids[spec["name"]],
            app_id=app_id,
            table_name=spec["name"],
            write_disposition="merge",
            primary_key=spec["primary_key"],
            **spec.get("api_params", {}),
        )
        resource.add_map(drop_stat_inf)
        resources.append(resource)
    return estat_source(tables=resources, app_id=app_id)
