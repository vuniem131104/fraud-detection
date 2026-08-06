-- =====================================================================
-- Flink job: FEATURE REAL-TIME cho entity CHIA SẺ (merchant, device).
--
-- Đọc topic `transactions` -> window aggregation -> hai topic upsert-kafka:
--     merchant_rt_10min   HOP(size 10 phút, slide 1 phút)  GROUP BY merchant_id
--     device_rt_1h        HOP(size 1 giờ,  slide 5 phút)   GROUP BY device_id
--
-- Vì sao ĐÚNG những feature này thuộc về Flink
-- --------------------------------------------
-- Flink có độ trễ cố hữu ~ (slide + watermark) ≈ 2,5 phút: window chỉ phát ra sau
-- khi watermark vượt window_end. Nên tiêu chí phân vai KHÔNG phải "window dài thì
-- dùng Flink" mà là **tốc độ thay đổi của tín hiệu so với độ trễ đó**:
--
--   card_tx_count_5min          1 -> 20 trong 90 giây  => trễ 2,5' làm MẤT cả sự kiện
--   merch_distinct_cards_10min  150 -> 180 trong 2,5'  => model không quan tâm
--   device_distinct_users_1h    8 -> 9                 => không đáng kể
--
-- Card burst là SỰ KIỆN TỨC THỜI (-> tính đồng bộ bằng Redis sorted set trong
-- đường score). Merchant bị lạm dụng là TRẠNG THÁI kéo dài 20+ phút -> Flink.
--
-- Và đây là thứ DUY NHẤT chỉ Flink làm được: giao dịch của một thẻ không thể biết
-- merchant này 10 phút qua bị 300 thẻ khác nhau quẹt mỗi thẻ đúng một lần. Đó
-- chính là dấu vân tay của bot card-testing.
--
-- Ba lỗi streaming được xử lý ở đây
-- ---------------------------------
--   burst        Flink backpressure tự chịu; tăng parallelism nếu cần
--   late arrival WATERMARK ... - INTERVAL '90' SECOND (producer trễ tối đa 60s)
--   duplicate    ROW_NUMBER() OVER (PARTITION BY id ORDER BY rowtime ASC) = 1
--
-- MỘT job, HAI sink (EXECUTE STATEMENT SET): source chỉ đọc Kafka một lần và
-- dedup một lần cho cả hai nhánh. Tách hai file sẽ thành hai job, hai consumer
-- group, đọc trùng cùng topic.
--
-- Độ dài window phải khớp data_pipelines/shared/feature_windows.py
-- (MERCHANT_RT_WINDOW_S=600, DEVICE_RT_WINDOW_S=3600, FLINK_WATERMARK_S=90).
-- SQL không import Python được nên đây là chỗ DUY NHẤT lặp lại các con số đó.
--
-- File này là TEMPLATE: Flink SQL KHÔNG nội suy biến môi trường, nên
-- ${KAFKA_BOOTSTRAP} và ${KAFKA_SQL_SECURITY} phải được thay TRƯỚC khi submit.
-- Dùng script bọc sẵn (nó lo phần thay biến rồi gọi sql-client):
--
--   docker compose exec flink-jobmanager /opt/flink/sql/submit.sh
--
-- Local: KAFKA_SQL_SECURITY rỗng -> ra đúng file y như trước khi template hoá.
-- GCP  : KAFKA_SECURITY_PROTOCOL=SASL_SSL -> script tự thêm các property SASL.
-- =====================================================================

SET 'execution.runtime-mode' = 'streaming';
SET 'pipeline.name' = 'realtime_features';
SET 'parallelism.default' = '2';

-- IDLE SOURCE: watermark toàn cục = MIN của mọi subtask. Nếu một partition Kafka
-- không có message (lưu lượng thấp — ở đây chỉ ~817 giao dịch/ngày!) thì subtask
-- đó giữ watermark ở mức thấp nhất và window KHÔNG BAO GIỜ đóng: source đọc được
-- hàng nghìn record mà output bằng 0. Đánh dấu source "idle" sau 10s im lặng.
SET 'table.exec.source.idle-timeout' = '10s';

-- CHECKPOINTING: job này là service chạy 24/7. Không checkpoint thì mỗi lần
-- restart mất toàn bộ state (buffer window + state dedup) VÀ mất vị trí offset
-- Kafka -> feature hụt cả khoảng thời gian job chết.
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.min-pause' = '5s';
SET 'execution.checkpointing.timeout' = '2min';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
SET 'restart-strategy.type' = 'fixed-delay';
SET 'restart-strategy.fixed-delay.attempts' = '10';
SET 'restart-strategy.fixed-delay.delay' = '10s';

-- --------------------------------------------------------------- source
CREATE TABLE transactions_src (
  id                   STRING,
  user_id              STRING,
  card_id              STRING,
  merchant_id          STRING,
  device_id            STRING,
  amount_usd           DOUBLE,
  currency             STRING,
  channel              STRING,
  billing_country_code STRING,
  ip_country_code      STRING,
  email_purchaser      STRING,
  email_recipient      STRING,
  created_at           TIMESTAMP(3),
  auth_3ds_flag        BOOLEAN,
  -- (2) LATE ARRIVAL: chờ 90s cho message về muộn (producer trễ tối đa 60s).
  -- Đánh đổi: chờ lâu = bắt được nhiều hàng muộn nhưng feature ra chậm hơn.
  WATERMARK FOR created_at AS created_at - INTERVAL '90' SECOND
) WITH (
  'connector'                      = 'kafka',
  'topic'                          = 'transactions',
${KAFKA_SQL_SECURITY}  'properties.bootstrap.servers'   = '${KAFKA_BOOTSTRAP}',
  'properties.group.id'            = 'flink-realtime-features',
  -- group-offsets: lần start mới tiếp tục từ offset đã commit thay vì nhảy tới
  -- cuối topic (latest-offset làm MẤT dữ liệu đến trong lúc job chết). Khi restore
  -- từ checkpoint thì Flink tự dùng offset trong checkpoint.
  'scan.startup.mode'              = 'group-offsets',
  'properties.auto.offset.reset'   = 'earliest',
  'format'                         = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'json.ignore-parse-errors'       = 'true'
);

-- ---------------------------------------------------------------- sinks
-- upsert-kafka: mỗi key giữ đúng giá trị mới nhất -> bridge đọc tới đâu push tới
-- đó là online store luôn tươi. Topic phải cấu hình cleanup.policy=compact
-- (xem redpanda-init trong docker-compose.yml).
CREATE TABLE merchant_rt_sink (
  merchant_id                STRING,
  window_end                 TIMESTAMP(3),
  merch_tx_count_10min       BIGINT,
  merch_distinct_cards_10min BIGINT,
  merch_amount_avg_10min     DOUBLE,
  PRIMARY KEY (merchant_id) NOT ENFORCED
) WITH (
  'connector'                    = 'upsert-kafka',
  'topic'                        = 'merchant_rt_10min',
${KAFKA_SQL_SECURITY}  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
  'key.format'                   = 'json',
  'value.format'                 = 'json'
);

CREATE TABLE device_rt_sink (
  device_id                STRING,
  window_end               TIMESTAMP(3),
  device_tx_count_1h       BIGINT,
  device_distinct_users_1h BIGINT,
  device_distinct_cards_1h BIGINT,
  PRIMARY KEY (device_id) NOT ENFORCED
) WITH (
  'connector'                    = 'upsert-kafka',
  'topic'                        = 'device_rt_1h',
${KAFKA_SQL_SECURITY}  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
  'key.format'                   = 'json',
  'value.format'                 = 'json'
);

-- ------------------------------------------------- (3) DEDUP duplicate
-- Giữ bản ghi ĐẦU TIÊN của mỗi id. `ORDER BY created_at ASC` là quan trọng: Flink
-- nhận ra đây là "Deduplicate keep-first-row" trên rowtime và cho ra stream
-- APPEND-ONLY. Nếu ORDER BY ... DESC thì thành keep-last-row -> changelog có
-- retract, và window aggregation phía sau sẽ không nhận được input append-only.
CREATE TEMPORARY VIEW tx_dedup AS
SELECT id, user_id, card_id, merchant_id, device_id, amount_usd, created_at
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at ASC) AS rn
  FROM transactions_src
)
WHERE rn = 1;

-- --------------------------------------------- WINDOW PROCESSING (x2)
EXECUTE STATEMENT SET
BEGIN

-- MERCHANT, cửa sổ 10 phút.
--   Đo trên dữ liệu sinh ra: cửa sổ 10 phút của một merchant có >=5 giao dịch thì
--   92% là fraud, trong khi nền chỉ 0,3%. Hai cột phải đọc CÙNG NHAU:
--     count cao + distinct_cards thấp  -> một thẻ bị quẹt lặp lại (card_testing)
--     count cao + distinct_cards cao   -> bot quét cả bộ thẻ trộm
--     count = 1                        -> khách bình thường
--   ODFV tính tỉ lệ hai cột thành `merchant_spread`.
INSERT INTO merchant_rt_sink
SELECT
  merchant_id,
  window_end,
  COUNT(*)                 AS merch_tx_count_10min,
  COUNT(DISTINCT card_id)  AS merch_distinct_cards_10min,
  AVG(amount_usd)          AS merch_amount_avg_10min
FROM TABLE(
  HOP(TABLE tx_dedup, DESCRIPTOR(created_at),
      INTERVAL '1' MINUTE,      -- slide: cập nhật mỗi phút
      INTERVAL '10' MINUTE)     -- size : MERCHANT_RT_WINDOW_S = 600
)
GROUP BY merchant_id, window_start, window_end;

-- DEVICE, cửa sổ 1 giờ.
--   Bản real-time của device_distinct_users_30d: batch nói "device này lịch sử
--   đáng ngờ", Flink nói "device này ĐANG bùng nổ ngay lúc này". distinct CARDS
--   mạnh hơn distinct users cho fraud ring (farm quay vòng thẻ trộm nhưng chỉ
--   dựng vài "user").
INSERT INTO device_rt_sink
SELECT
  device_id,
  window_end,
  COUNT(*)                 AS device_tx_count_1h,
  COUNT(DISTINCT user_id)  AS device_distinct_users_1h,
  COUNT(DISTINCT card_id)  AS device_distinct_cards_1h
FROM TABLE(
  HOP(TABLE tx_dedup, DESCRIPTOR(created_at),
      INTERVAL '5' MINUTE,      -- slide: 5 phút (1 giờ / 5' = 12 window/record)
      INTERVAL '1' HOUR)        -- size : DEVICE_RT_WINDOW_S = 3600
)
GROUP BY device_id, window_start, window_end;

END;
