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
    GOOGLE_MANAGED_KAFKA_AUTH_PRINCIPAL   ghi đè principal (mặc định: email SA)
"""

from __future__ import annotations

import os
from datetime import timezone

# Scope duy nhất Managed Kafka nhận.
_GCP_SCOPE = "https://www.googleapis.com/auth/cloud-platform"


def bootstrap() -> str:
    """``KAFKA_BOOTSTRAP`` — không có mặc định, sai là mọi service im lặng chờ."""
    b = os.environ.get("KAFKA_BOOTSTRAP")
    if not b:
        raise RuntimeError("Thiếu KAFKA_BOOTSTRAP")
    return b


def _metadata(path: str) -> str:
    """Đọc một trường từ metadata server của GCE."""
    import urllib.request

    req = urllib.request.Request(
        "http://metadata.google.internal/computeMetadata/v1/" + path,
        headers={"Metadata-Flavor": "Google"})
    return urllib.request.urlopen(req, timeout=5).read().decode().strip()


def _principal(creds) -> str:
    """Email của service account — SASL principal mà Managed Kafka đòi.

    librdkafka bắt buộc principal khác rỗng. Trên GCE, ``google.auth.default()``
    trả về ComputeEngineCredentials với ``service_account_email`` là "default"
    cho tới khi refresh, nên phải hỏi metadata server.
    """
    if p := os.environ.get("GOOGLE_MANAGED_KAFKA_AUTH_PRINCIPAL"):
        return p
    email = getattr(creds, "service_account_email", None)
    if email and email != "default":
        return email
    return _metadata("instance/service-accounts/default/email")


def _oauth_token_cb(_config: str):
    """Trả 4-tuple ``(token, expiry_epoch_giây, principal, extensions)``.

    ĐÚNG SỐ PHẦN TỬ LÀ BẮT BUỘC: hợp đồng ``oauth_cb`` của confluent-kafka là
    4-tuple. Trả 2-tuple ``(token, expiry)`` thì broker từ chối với
    "Authentication failed ... invalid credentials with SASL mechanism OAUTHBEARER"
    — lỗi không nói gì về hình dạng tuple nên rất dễ đi tìm sai chỗ.

    Dùng ADC: trên VM GCP đó là service account gắn kèm, không cần key file.
    SA cần role ``roles/managedkafka.client``.
    """
    import google.auth
    import google.auth.transport.requests

    creds, _ = google.auth.default(scopes=[_GCP_SCOPE])
    creds.refresh(google.auth.transport.requests.Request())

    # creds.expiry là datetime NAIVE biểu diễn UTC. Gọi .timestamp() trực tiếp sẽ
    # được hiểu là giờ ĐỊA PHƯƠNG — container chạy TZ=Asia/Ho_Chi_Minh nên token
    # trông như đã hết hạn 7 tiếng trước và librdkafka loại nó.
    expiry = creds.expiry
    if expiry.tzinfo is None:
        expiry = expiry.replace(tzinfo=timezone.utc)
    return creds.token, expiry.timestamp(), _principal(creds), {}


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
