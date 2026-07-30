-- =====================================================================
-- Bảng RỖNG có chủ đích — "batch_source" hợp lệ cho hai PushSource của Feast.
--
-- Bối cảnh
-- --------
-- Hai nhóm feature real-time (merchant 10 phút, device 1 giờ) do **Flink**
-- tính và đẩy thẳng vào Redis. Feast lại bắt buộc mọi ``PushSource`` phải khai
-- một ``batch_source``. Nếu trỏ batch_source vào bảng CÓ dữ liệu thì một lệnh
-- ``feast materialize`` lỡ tay sẽ đọc bảng đó và **ghi đè giá trị Flink vừa
-- đẩy** — serving đọc số của đêm qua trong khi merchant đang bị quét thẻ.
--
-- Vì sao giá trị batch luôn sai với nhóm này:
--   cửa sổ 10 phút / 1 giờ NGẮN HƠN nhịp chạy batch (24h), nên bản batch chậm
--   tối đa 24 giờ -> không phải "hơi cũ" mà là vô nghĩa.
--
-- Nên hai bảng này để RỖNG:
--   * đủ để ``feast apply`` chạy được (schema hợp lệ),
--   * ``feast materialize`` nếu lỡ chạm vào thì đọc 0 dòng -> no-op, không phá gì.
--
-- Bản offline THẬT của hai nhóm feature này nằm trong ``application.feat_training``
-- (Spark tính point-in-time cho từng giao dịch), giống mọi feature khác.
--
-- Xem thêm: feature_store/feature_views.py (BATCH_VIEWS / PUSH_VIEWS).
-- Idempotent: chạy lại nhiều lần không lỗi.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS application;

-- Flink: HOP(size 10', slide 1') GROUP BY merchant_id
CREATE TABLE IF NOT EXISTS application.feat_merchant_rt (
    merchant_id                TEXT             NOT NULL,
    merch_tx_count_10min       BIGINT,
    merch_distinct_cards_10min BIGINT,
    merch_amount_avg_10min     DOUBLE PRECISION,
    event_timestamp            TIMESTAMPTZ      NOT NULL,
    created                    TIMESTAMPTZ      NOT NULL
);

-- Flink: HOP(size 1h, slide 5') GROUP BY device_id
CREATE TABLE IF NOT EXISTS application.feat_device_rt (
    device_id                TEXT        NOT NULL,
    device_tx_count_1h       BIGINT,
    device_distinct_users_1h BIGINT,
    device_distinct_cards_1h BIGINT,
    event_timestamp          TIMESTAMPTZ NOT NULL,
    created                  TIMESTAMPTZ NOT NULL
);
