"""Feast feature service — hợp đồng feature DUY NHẤT cho train và serve.

Training gọi ``get_historical_features(service)``; serving gọi
``get_online_features(service)``. Cùng một service -> cùng tên, cùng thứ tự, cùng
định nghĩa feature ở hai bên.

Phạm vi bảo đảm của Feast — nói rõ để không tưởng nó lo hết:

* 4 view **batch** + ODFV: Feast bảo đảm hoàn toàn.
* 2 view **Flink**: Feast bảo đảm phần đọc; phần *tính* thì Spark
  (``feat_training``) và Flink (``realtime_features.sql``) là hai cài đặt khác nhau
  của cùng một định nghĩa. Độ dài cửa sổ khai ở
  ``data_pipelines/shared/feature_windows.py``.
* Velocity 5 phút: phần tính nằm ở code API (Redis sorted set) và Spark.
  ``tests/test_velocity_parity.py`` là thứ canh cho hai bên khớp nhau.

Model chỉ nên đọc feature qua service này, không đọc trực tiếp từng view — nếu
không thì thứ tự cột lúc train và lúc serve có thể lệch mà không ai biết.
"""

from feast import FeatureService

from feature_views import (
    card_features,
    device_features,
    device_realtime,
    merchant_features,
    merchant_realtime,
    txn_on_demand,
    user_features,
)

fraud_detection_service = FeatureService(
    name="fraud_detection_service",
    features=[
        card_features,       # batch : dim thẻ + baseline 7d/90d
        user_features,       # batch : dim khách + baseline/graph 30d
        merchant_features,   # batch : dim merchant + baseline 30d
        device_features,     # batch : graph 30d (fraud ring)
        merchant_realtime,   # Flink : 10 phút (card testing đang diễn ra)
        device_realtime,     # Flink : 1 giờ (ring đang hình thành)
        txn_on_demand,       # on-demand: tỉ lệ, tuổi, gate độ tươi, velocity 5'
    ],
)
