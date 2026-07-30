"""Feast data sources — trỏ vào offline store (Postgres schema ``application``).

Hai loại nguồn, tương ứng hai người ghi khác nhau vào online store:

* **4 ``PostgreSQLSource``** — bảng ``feat_*`` do DP3a (Spark) sinh mỗi ngày.
  ``feast materialize`` đọc chúng và đẩy lên Redis.

* **2 ``PushSource``** — feature real-time do Flink tính; bridge gọi ``store.push()``
  đẩy thẳng vào Redis. Feast bắt buộc PushSource phải khai ``batch_source``, nên
  chúng trỏ vào hai bảng ``feat_*_rt`` **cố tình để RỖNG**: nếu ai đó lỡ chạy
  ``feast materialize`` trên hai view này thì đọc 0 dòng -> no-op, không ghi đè giá
  trị mà Flink vừa đẩy. Bản offline THẬT của nhóm này nằm trong
  ``application.feat_training`` như mọi feature khác.
  Xem ``data_pipelines/sql/warehouse/02_realtime_placeholders.sql``.
"""

from feast import PushSource
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
    PostgreSQLSource,
)


def _pg(name: str, table: str) -> PostgreSQLSource:
    """PostgreSQLSource chuẩn cho bảng ``application.<table>``."""
    return PostgreSQLSource(
        name=name,
        query=f"SELECT * FROM application.{table}",
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )


# ------------------------------------------------------------------- batch
card_source = _pg("card_source", "feat_card")
user_source = _pg("user_source", "feat_user")
merchant_source = _pg("merchant_source", "feat_merchant")
device_source = _pg("device_source", "feat_device")

# --------------------------------------------------------------- streaming
merchant_rt_push_source = PushSource(
    name="merchant_rt_push_source",
    batch_source=_pg("merchant_rt_batch", "feat_merchant_rt"),   # RỖNG có chủ đích
)

device_rt_push_source = PushSource(
    name="device_rt_push_source",
    batch_source=_pg("device_rt_batch", "feat_device_rt"),       # RỖNG có chủ đích
)
