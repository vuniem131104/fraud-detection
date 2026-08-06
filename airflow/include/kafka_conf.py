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

import base64
import json
import time
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


# Managed Kafka KHÔNG nhận access token thô. Nó đòi một JWT gói quanh token, đúng
# như GcpLoginCallbackHandler của Google dựng (đã dịch ngược từ
# managed-kafka-auth-login-handler-1.0.6):
#
#   b64url({"typ":"JWT","alg":"GOOG_OAUTH2_TOKEN"})
#   + "." + b64url({"exp":<hết hạn>,"iat":<bây giờ>,"scope":"kafka","sub":<email SA>})
#   + "." + b64url(<access token>)
#
# base64url KHÔNG padding, ba phần nối bằng dấu chấm. Gửi access token thô sẽ bị
# broker trả "invalid credentials with SASL mechanism OAUTHBEARER" — thông báo
# không hề gợi ý rằng vấn đề là ĐỊNH DẠNG chứ không phải quyền.
_JWT_HEADER = {"typ": "JWT", "alg": "GOOG_OAUTH2_TOKEN"}


def _b64(raw: str) -> str:
    """base64url không padding — giống Base64.getUrlEncoder().withoutPadding()."""
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def _kafka_access_token(token: str, exp_epoch: int, subject: str) -> str:
    """Gói access token thành JWT mà Managed Kafka chấp nhận.

    ``scope`` là chuỗi cố định ``"kafka"`` (không phải scope OAuth dùng để LẤY
    token — cái đó là cloud-platform).
    """
    claims = {"exp": int(exp_epoch), "iat": int(time.time()),
              "scope": "kafka", "sub": subject}
    return ".".join([
        _b64(json.dumps(_JWT_HEADER, separators=(",", ":"))),
        _b64(json.dumps(claims, separators=(",", ":"))),
        _b64(token),
    ])


def _oauth_token_cb(_config: str):
    """Trả 4-tuple ``(token, expiry_epoch_giây, principal, extensions)``.

    Hai chỗ dễ sai, cả hai đều báo cùng một lỗi "invalid credentials":
      1. Số phần tử: hợp đồng ``oauth_cb`` là 4-tuple, không phải 2.
      2. Định dạng token: phải là JWT bọc quanh access token (xem
         ``_kafka_access_token``), không phải access token thô.

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
    exp = expiry.timestamp()

    principal = _principal(creds)
    return _kafka_access_token(creds.token, exp, principal), exp, principal, {}


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
