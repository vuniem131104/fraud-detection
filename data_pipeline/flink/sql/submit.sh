#!/bin/sh
# =====================================================================
# Submit job realtime_features lên Flink session cluster.
#
#   docker compose exec flink-jobmanager /opt/flink/sql/submit.sh
#
# VÌ SAO CẦN SCRIPT NÀY: Flink SQL không nội suy biến môi trường, mà bootstrap của
# Managed Kafka lẫn 4 property SASL đều nằm trong .env. Script thay biến vào
# realtime_features.sql (template) rồi mới gọi sql-client.
#
# Không dùng envsubst: image flink không có sẵn. sed đủ vì chỉ có 2 biến.
#
# Biến môi trường:
#   KAFKA_BOOTSTRAP        bắt buộc
#   KAFKA_SASL_MECHANISM   OAUTHBEARER (mặc định) | PLAIN | SCRAM-SHA-512
#   KAFKA_SASL_USERNAME / KAFKA_SASL_PASSWORD    chỉ cho PLAIN / SCRAM
# =====================================================================
set -e

SQL_DIR="$(dirname "$0")"
TEMPLATE="$SQL_DIR/realtime_features.sql"
# /opt/flink/sql mount read-only -> render ra /tmp.
RENDERED="/tmp/realtime_features.rendered.sql"

if [ -z "$KAFKA_BOOTSTRAP" ]; then
  echo "LỖI: thiếu KAFKA_BOOTSTRAP" >&2
  exit 1
fi
MECHANISM="${KAFKA_SASL_MECHANISM:-OAUTHBEARER}"

# --- khối property bảo mật, chèn vào cả 3 bảng Kafka -----------------
SECURITY="  'properties.security.protocol' = 'SASL_SSL',\n"
SECURITY="$SECURITY  'properties.sasl.mechanism' = '$MECHANISM',\n"
if [ "$MECHANISM" = "OAUTHBEARER" ]; then
  # Class này nằm trong managed-kafka-auth-login-handler-*-all.jar, phải có ở
  # /opt/flink/lib của CẢ jobmanager lẫn taskmanager. Thiếu thì job submit được
  # nhưng chết lúc khởi tạo consumer.
  SECURITY="$SECURITY  'properties.sasl.login.callback.handler.class' = 'com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler',\n"
  SECURITY="$SECURITY  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;',\n"
else
  SECURITY="$SECURITY  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${KAFKA_SASL_USERNAME}\" password=\"${KAFKA_SASL_PASSWORD}\";',\n"
fi

# Dấu | làm delimiter: giá trị chứa / (jaas config) nhưng không chứa |
#
# Địa chỉ /^[[:space:]]*--/! : CHỈ thay ở dòng KHÔNG phải comment. Phần header của
# template có nhắc tên hai placeholder, mà KAFKA_SQL_SECURITY nở ra 4 dòng -> ba
# dòng sau tràn khỏi comment và thành SQL rác; sql-client chết với
# "Non-query expression encountered in illegal context". Lọc theo dòng comment thì
# tài liệu trong template muốn nhắc placeholder bao nhiêu lần cũng được.
sed -e "/^[[:space:]]*--/! s|\${KAFKA_BOOTSTRAP}|$KAFKA_BOOTSTRAP|g" \
    -e "/^[[:space:]]*--/! s|\${KAFKA_SQL_SECURITY}|$SECURITY|g" \
    "$TEMPLATE" > "$RENDERED"

# Placeholder còn sót ở dòng SQL = job sẽ submit với bootstrap sai. Chặn tại đây,
# vì lỗi kiểu đó chỉ lộ ra ở taskmanager sau khi submit thành công.
if grep -v '^[[:space:]]*--' "$RENDERED" | grep -q '\${KAFKA_'; then
  echo "LỖI: còn placeholder chưa thay trong $RENDERED" >&2
  grep -n -v '^[[:space:]]*--' "$RENDERED" | grep '\${KAFKA_' >&2
  exit 1
fi

echo "=> Kafka: $KAFKA_BOOTSTRAP (SASL_SSL/$MECHANISM)"
echo "=> Đã render: $RENDERED"
exec /opt/flink/bin/sql-client.sh -f "$RENDERED"
