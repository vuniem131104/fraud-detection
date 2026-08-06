-- =====================================================================
-- DB vận hành của TEAM DATA (opsdb) — bảng landing nhận event từ Kafka.
--
-- Đây là system of record cho luồng batch: file ngày được dump ra từ bảng
-- này (không replay Kafka), nên chạy lại dump bao nhiêu lần cũng ra kết quả
-- y hệt và không phụ thuộc retention của Kafka.
--
-- CỐ Ý KHÔNG CÓ PRIMARY KEY: Kafka giao hàng at-least-once nên duplicate
-- (~1.5% producer tiêm vào) phải nằm nguyên trong bảng -> chảy xuống file ->
-- Bronze giữ thô -> DP2 (Spark) khử. Nếu đặt PK thì Postgres chặn hết và mất
-- luôn phần "duplicate" của rubric offline.
--
-- Idempotent: chạy lại nhiều lần không lỗi.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.transactions (
    id                   TEXT             NOT NULL,
    user_id              TEXT             NOT NULL,
    card_id              TEXT             NOT NULL,
    merchant_id          TEXT             NOT NULL,
    device_id            TEXT             NOT NULL,
    amount_usd           DOUBLE PRECISION NOT NULL,
    currency             TEXT,
    channel              TEXT,
    billing_country_code TEXT,
    ip_country_code      TEXT,
    email_purchaser      TEXT,
    email_recipient      TEXT,
    created_at           TIMESTAMPTZ      NOT NULL,   -- event time của giao dịch
    auth_3ds_flag        BOOLEAN,                     -- có từ sau mốc cutover
    ingested_at          TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

-- dump job query theo khoảng created_at của 1 ngày -> index này là bắt buộc
CREATE INDEX IF NOT EXISTS transactions_created_idx
    ON ops.transactions (created_at);
