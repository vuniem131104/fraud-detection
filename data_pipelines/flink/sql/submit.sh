#!/bin/sh
# =====================================================================
# Submit job realtime_features lên Flink session cluster.
#
#   docker compose exec flink-jobmanager /opt/flink/sql/submit.sh
#
# VÌ SAO CẦN SCRIPT NÀY: Flink SQL không nội suy biến môi trường. Broker Kafka
# khác nhau giữa local (redpanda:29092, plaintext) và GCP (Managed Kafka,
# SASL_SSL/OAUTHBEARER), nên realtime_features.sql là template và script này
# thay biến rồi mới gọi sql-client.
#
# Không dùng envsubst: image flink không có sẵn nó. sed đủ dùng vì chỉ có 2 biến.
#
# Biến môi trường:
#   KAFKA_BOOTSTRAP          mặc định redpanda:29092
#   KAFKA_SECURITY_PROTOCOL  PLAINTEXT (mặc định) | SASL_SSL
#   KAFKA_SASL_MECHANISM     OAUTHBEARER (mặc định khi SASL) | PLAIN | SCRAM-SHA-512
#   KAFKA_SASL_USERNAME/PASSWORD   chỉ cho PLAIN / SCRAM
# =====================================================================
set -e

SQL_DIR="$(dirname "$0")"
TEMPLATE="$SQL_DIR/realtime_features.sql"
# /opt/flink/sql được mount read-only -> render ra /tmp.
RENDERED="/tmp/realtime_features.rendered.sql"

BOOTSTRAP="${KAFKA_BOOTSTRAP:-redpanda:29092}"
PROTOCOL="${KAFKA_SECURITY_PROTOCOL:-PLAINTEXT}"
MECHANISM="${KAFKA_SASL_MECHANISM:-OAUTHBEARER}"

# --- khối property bảo mật, chèn vào cả 3 bảng Kafka -----------------
# Local (PLAINTEXT): rỗng -> file render ra giống y bản trước khi template hoá.
SECURITY=""
case "$PROTOCOL" in
  PLAINTEXT) ;;
  *)
    SECURITY="  'properties.security.protocol' = '$PROTOCOL',\\n"
    case "$PROTOCOL" in
      SASL_*)
        SECURITY="$SECURITY  'properties.sasl.mechanism' = '$MECHANISM',\\n"
        if [ "$MECHANISM" = "OAUTHBEARER" ]; then
          # Managed Kafka của GCP: handler lấy access token từ ADC (service
          # account gắn trên VM). Class này nằm trong thư viện
          # managed-kafka-auth-login-handler, PHẢI thả jar vào /opt/flink/lib
          # cùng chỗ với flink-sql-connector-kafka — không có nó thì job fail
          # lúc khởi tạo consumer, không phải lúc submit.
          SECURITY="$SECURITY  'properties.sasl.login.callback.handler.class' = 'com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler',\\n"
          SECURITY="$SECURITY  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;',\\n"
        else
          SECURITY="$SECURITY  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${KAFKA_SASL_USERNAME}\" password=\"${KAFKA_SASL_PASSWORD}\";',\\n"
        fi
        ;;
    esac
    ;;
esac

# Dấu | làm delimiter: giá trị chứa / (jaas config) nhưng không chứa |
sed -e "s|\${KAFKA_BOOTSTRAP}|$BOOTSTRAP|g" \
    -e "s|\${KAFKA_SQL_SECURITY}|$SECURITY|g" \
    "$TEMPLATE" > "$RENDERED"

echo "=> Kafka: $BOOTSTRAP ($PROTOCOL${SECURITY:+/$MECHANISM})"
echo "=> Đã render: $RENDERED"
exec /opt/flink/bin/sql-client.sh -f "$RENDERED"
