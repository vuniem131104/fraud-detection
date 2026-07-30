-- =====================================================================
-- Ground truth cho training (warehouse của TEAM ML).
--
-- Label KHÔNG phải feature và không cần đi qua medallion: nó là nhãn để
-- train, nạp thẳng vào warehouse. Ngoài đời label đến từ chargeback /
-- analyst review, trễ 30-120 ngày; MVP giả định label tức thời (xem
-- docs/"Data Engineering cho dan ML.md" mục "Còn thiếu gì").
--
-- Idempotent: chạy lại nhiều lần không lỗi.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS application;

CREATE TABLE IF NOT EXISTS application.labels (
    transaction_id TEXT        NOT NULL,
    label          SMALLINT    NOT NULL,   -- 0 = hợp lệ, 1 = gian lận
    label_source   TEXT,                   -- chargeback / manual_review / rule_engine
    created_at     TIMESTAMPTZ NOT NULL,   -- thời điểm label được xác nhận
    CONSTRAINT labels_value_check CHECK (label IN (0, 1))
);

-- notebook join labels theo transaction_id
CREATE INDEX IF NOT EXISTS labels_txn_idx ON application.labels (transaction_id);
CREATE INDEX IF NOT EXISTS labels_label_idx ON application.labels (label);
