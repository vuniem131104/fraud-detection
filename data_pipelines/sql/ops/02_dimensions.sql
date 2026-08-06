-- =====================================================================
-- Bảng REFERENCE DATA của TEAM DATA (opsdb, schema ops).
--
-- Bốn bảng này là "master data" của hệ vận hành: khách hàng, thẻ, đơn vị chấp
-- nhận thẻ, thiết bị. Khác transactions ở hai điểm quan trọng:
--
--   1. CÓ PRIMARY KEY — đây là bảng trạng thái (một dòng = một thực thể),
--      không phải log sự kiện. Không có duplicate để giữ lại.
--   2. ĐỔI CHẬM (slowly changing) — user đổi nước, KYC được nâng cấp, thẻ
--      chuyển sang virtual. Team data export FULL SNAPSHOT mỗi ngày; DP2 so
--      snapshot hôm nay với bản đang current ở Gold để dựng SCD Type 2.
--
-- Vì sao phải là full snapshot mỗi ngày chứ không phải "nạp một lần":
--   SCD2 chỉ có việc để làm khi nó THẤY được thay đổi. Nạp một lần thì mỗi
--   thực thể đúng một version, cột valid_from_ts/valid_to_ts/is_current tồn
--   tại nhưng không chứng minh được gì.
--
-- Idempotent: chạy lại nhiều lần không lỗi.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS ops;

-- --------------------------------------------------------------- users
CREATE TABLE IF NOT EXISTS ops.users (
    id               TEXT        PRIMARY KEY,
    email            TEXT,
    country_code     TEXT,
    customer_segment TEXT,                    -- normal / premium / vip
    kyc_level        SMALLINT,                -- 0..3
    email_verified   BOOLEAN,
    created_at       TIMESTAMPTZ NOT NULL,    -- ngày mở tài khoản
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- --------------------------------------------------------------- cards
CREATE TABLE IF NOT EXISTS ops.cards (
    id           TEXT        PRIMARY KEY,
    user_id      TEXT        NOT NULL,
    issuer_code  TEXT,
    country_code TEXT,
    brand        TEXT,                        -- visa / mastercard / amex ...
    type         TEXT,                        -- debit / credit / prepaid
    bin_code     TEXT,
    is_virtual   BOOLEAN,
    created_at   TIMESTAMPTZ NOT NULL,        -- ngày phát hành thẻ
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS cards_user_idx ON ops.cards (user_id);

-- ----------------------------------------------------------- merchants
CREATE TABLE IF NOT EXISTS ops.merchants (
    id           TEXT        PRIMARY KEY,
    name         TEXT,
    category     TEXT,
    country_code TEXT,
    risk_level   SMALLINT,                    -- 1..5, do team risk gán
    created_at   TIMESTAMPTZ NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ------------------------------------------------------------- devices
CREATE TABLE IF NOT EXISTS ops.devices (
    id                TEXT        PRIMARY KEY,
    fingerprint       TEXT,
    device_type       TEXT,                   -- mobile / desktop / tablet
    os                TEXT,
    browser           TEXT,
    screen_resolution TEXT,
    created_at        TIMESTAMPTZ NOT NULL,   -- lần đầu hệ thấy thiết bị này
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
