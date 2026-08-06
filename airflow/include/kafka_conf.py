"""Cấu hình client Kafka cho Managed Service for Apache Kafka.

Ba service streaming (``kafka_to_ops``, ``feature_bridge``, ``generate_stream``) đều
dựng Consumer/Producer của ``confluent_kafka`` và phải nói cùng một giao thức. Gom
vào một chỗ để không service nào bị bỏ sót khi đổi.

Managed Kafka bắt buộc **SASL_SSL + OAUTHBEARER** với access token của service
account. librdkafka không tự lấy token được nên phải đưa vào một callback refresh —
đó là toàn bộ lý do tồn tại của ``_oauth_token_cb``. Token sống 1 giờ và librdkafka
gọi lại callback trước khi hết hạn, nên không cần tự hẹn giờ.

Biến môi trường:
    KAFKA_BOOTSTRAP     bootstrap.servers (bắt buộc)
    KAFKA_SASL_MECHANISM   OAUTHBEARER (mặc định) | PLAIN | SCRAM-SHA-512
    KAFKA_SASL_USERNAME / KAFKA_SASL_PASSWORD   chỉ cho PLAIN / SCRAM
    KAFKA_SSL_CAFILE    CA tuỳ chọn (mặc định dùng CA hệ thống)
"""

from __future__ import annotations

import os

# Scope duy nhất Managed Kafka nhận.
_GCP_SCOPE = "https://www.googleapis.com/auth/cloud-platform"


def bootstrap() -> str:
    """``KAFKA_BOOTSTRAP`` — không có mặc định, sai là mọi service im lặng chờ."""
    b = os.environ.get("KAFKA_BOOTSTRAP")
    if not b:
        raise RuntimeError("Thiếu KAFKA_BOOTSTRAP")
    return b


def _oauth_token_cb(_config: str):
    """Trả ``(token, thời_điểm_hết_hạn)`` cho SASL/OAUTHBEARER.

    Dùng Application Default Credentials: trên VM GCP đó là service account gắn
    kèm, không cần key file. SA cần role ``roles/managedkafka.client``.
    """
    import google.auth
    import google.auth.transport.requests

    creds, _ = google.auth.default(scopes=[_GCP_SCOPE])
    creds.refresh(google.auth.transport.requests.Request())
    # librdkafka cần expiry dạng epoch giây.
    return creds.token, creds.expiry.timestamp()


def client_config(**extra) -> dict:
    """Config cho Producer/Consumer, đã gộp phần bảo mật.

    ``extra`` là các khoá riêng của từng service (group.id, linger.ms, ...) và luôn
    thắng giá trị mặc định ở đây.
    """
    mechanism = os.environ.get("KAFKA_SASL_MECHANISM", "OAUTHBEARER").upper()
    cfg: dict = {
        "bootstrap.servers": bootstrap(),
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": mechanism,
    }
    if ca := os.environ.get("KAFKA_SSL_CAFILE"):
        cfg["ssl.ca.location"] = ca
    if mechanism == "OAUTHBEARER":
        # Truyền HÀM, không phải token: token hết hạn sau 1 giờ mà mấy service này
        # chạy 24/7, nên lấy token một lần lúc khởi động là sai.
        cfg["oauth_cb"] = _oauth_token_cb
    else:
        cfg["sasl.username"] = os.environ.get("KAFKA_SASL_USERNAME", "")
        cfg["sasl.password"] = os.environ.get("KAFKA_SASL_PASSWORD", "")
    cfg.update(extra)
    return cfg


def describe() -> str:
    """Một dòng mô tả để in ra log lúc khởi động (không lộ secret)."""
    mechanism = os.environ.get("KAFKA_SASL_MECHANISM", "OAUTHBEARER").upper()
    return f"{bootstrap()} (SASL_SSL/{mechanism})"
