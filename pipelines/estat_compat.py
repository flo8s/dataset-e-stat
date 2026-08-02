"""estat-api-dlt-helper の互換パッチ。

小地域・メッシュ統計 (searchKind=2) の getStatsData レスポンスは TABLE_INF に
COLLECT_AREA を持たないが、estat-api-dlt-helper (<=0.3.1) はこれを必須として
parse に失敗する。upstream (udus122 fork fix/optional-collect-area) の修正が
リリースされるまでの暫定対応として collect_area を optional 化する。

このモジュールを import した時点でパッチが当たる。依存を上げたら
import ごと削除する。
"""

from estat_api_dlt_helper.models import estat_models

estat_models.TableInf.model_fields["collect_area"].default = None
estat_models.TableInf.model_rebuild(force=True)
