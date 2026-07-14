# Thiết kế: Tính feature on-demand lúc scoring

**Status:** Draft — chờ review
**Scope:** Serving path (`src/fraud_detection/`), Redis feature state (`src/workers/features/`), một thay đổi nhỏ ở ETL (`airflow/etl/feature_etl.py`)
**Không đổi:** Model, offline training pipeline, schema 29 feature

---

## 1. Vấn đề

Đường scoring hiện tại (`main`):

```
POST /score {transaction_id, user_id, card_id}
  → Feast online store: đọc dòng feature ĐÃ MATERIALIZE GẦN NHẤT của (user_id, card_id)
  → encode 29 features → KServe → probability
```

Dòng "gần nhất" đó là feature row của **giao dịch trước đó** (do batch ETL + materialize daily sinh ra). Hệ quả — hai lớp lỗi, nặng nhẹ khác nhau:

**Lỗi 1 — sai input (correctness):** các feature mô tả *chính giao dịch đang chấm* (`amount_usd`, `hour`, `is_night`, `geo_mismatch`, `foreign_ip`, `merchant_category`, `merchant_risk_level`, …) đang lấy giá trị của **giao dịch cũ**. Giao dịch $5,000 lúc 3h sáng từ IP nước ngoài tại merchant crypto được chấm bằng amount/giờ/geo/merchant của giao dịch mua cà phê $12 hôm qua. Đây là training/serving skew ở mức "model nhận nhầm input", không phải "input hơi cũ".

**Lỗi 2 — velocity cũ (freshness):** các feature vận tốc (`card_tx_count_1h`, `card_amount_sum_24h`, …) đứng yên giữa hai lần materialize (daily). Fraud burst — 10 giao dịch trong 5 phút — là pattern quan trọng nhất mà velocity feature tồn tại để bắt, và chính nó bị mù.

Batch ETL ([airflow/etl/feature_etl.py](../../airflow/etl/feature_etl.py)) **không có lỗi** — nó tính point-in-time đúng cho training. Vấn đề chỉ nằm ở chỗ serving tái sử dụng giá trị point-in-time của quá khứ làm giá trị hiện tại.

### Tài sản sẵn có (nhánh `main`)

- `RedisFeaturesRefresher` ([src/workers/features/redis_worker.py](../../src/workers/features/redis_worker.py)): Kafka consumer (KEDA scale theo lag) maintain ZSET giao dịch per (user, card) + Lua script atomic trong Redis. **Nhưng** nó tính feature của schema cũ (`no_transactions_30_days`, `D4`/`D15`) ghi vào key `user:card:features:*` mà serving không đọc → hiện là dead infrastructure, sẽ được tái chế.
- `daily_refresh` CronJob ([src/jobs/daily_refresh.py](../../src/jobs/daily_refresh.py)): housekeeping cho state cũ đó.
- Feast + materialize daily (nhánh `deploy/airflow-mlflow` đã thêm `materialize_incremental` vào DAG).

---

## 2. Nguyên tắc thiết kế

1. **Serving phải tính đúng cái training đã tính.** Mọi công thức on-demand phải khớp từng-feature với SQL trong `feature_etl.py` (bảng đối chiếu §5). Parity test là deliverable bắt buộc, không phải nice-to-have.
2. **Chia feature theo tốc độ thay đổi, chọn nguồn theo nhóm** — không ép một cơ chế cho cả 29 feature.
3. **Cold start phải trùng ngữ nghĩa training** — giao dịch đầu tiên của card trong training có `count=1, gap=NULL, zscore=NULL, seq=1`; Redis rỗng phải cho ra đúng như vậy (và thực tế là tự nhiên như vậy, xem §5.3).
4. **Redis lỗi → degrade, không chết:** velocity feature thành NaN (model LightGBM xử lý missing sẵn — cùng cơ chế `build_model_inputs` hiện tại), kèm alert.

---

## 3. Phân loại 29 feature và nguồn lúc serving

| # | Feature | Nguồn mới | Cách tính lúc serving |
|---|---------|-----------|----------------------|
| 1 | `amount_usd` | **Request** | passthrough |
| 2 | `log_amount` | **API derive** | `ln(1 + amount_usd)` |
| 3 | `hour` | **API derive** | giờ **UTC** từ `created_at` |
| 4 | `weekday` | **API derive** | `isodow − 1` (Thứ 2 = 0), **UTC** |
| 5 | `is_night` | **API derive** | `hour < 6 or hour >= 23` |
| 6 | `channel` | **Request** | passthrough |
| 7 | `card_brand` | Feast (giữ nguyên) | bất biến per card → dòng cuối luôn đúng |
| 8 | `card_type` | Feast (giữ nguyên) | như trên |
| 9 | `is_virtual` | Feast (giữ nguyên) | như trên |
| 10 | `customer_segment` | Feast (giữ nguyên) | đổi rất hiếm, daily đủ |
| 11 | `kyc_level` | Feast (giữ nguyên) | như trên |
| 12 | `email_verified` | Feast (giữ nguyên) | như trên |
| 13 | `merchant_category` | **Request** | client (gateway) biết merchant của giao dịch |
| 14 | `merchant_risk_level` | **Request** | như trên *(Phase 3: merchant feature view riêng)* |
| 15 | `account_age_days` | **API derive** | `floor((created_at − user_created_at)/86400)` — `user_created_at` từ Feast (cột mới, §6.1) |
| 16 | `card_age_days` | **API derive** | tương tự với `card_created_at` (cột mới) |
| 17 | `geo_mismatch` | **API derive** | `billing_country_code != ip_country_code` (None-safe, §5.1) |
| 18 | `foreign_ip` | **API derive** | `ip_country_code != user_country` — `user_country` từ Feast (cột mới) |
| 19 | `recipient_differs` | **API derive** | `recipient is not None and recipient != purchaser` |
| 20 | `card_tx_count_1h` | **Redis on-demand** | `ZCOUNT(card:tx, T−3600, T) + 1` |
| 21 | `card_tx_count_24h` | **Redis on-demand** | `ZCOUNT(card:tx, T−86400, T) + 1` |
| 22 | `card_amount_sum_24h` | **Redis on-demand** | `Σ amount(prior trong 24h) + amount_usd` |
| 23 | `card_seconds_since_last_tx` | **Redis on-demand** | `T − last_ts` (agg hash); NaN nếu chưa có |
| 24 | `card_amount_zscore` | **Redis on-demand** | từ `(cnt, sum, sumsq)` trọn đời — công thức §5.2 |
| 25 | `card_tx_seq` | **Redis on-demand** | `cnt_prior + 1` |
| 26 | `card_declines_24h` | **Redis on-demand** | `ZCOUNT(card:declines, T−86400, T)` |
| 27 | `user_tx_count_24h` | **Redis on-demand** | `ZCOUNT(user:tx, T−86400, T) + 1` |
| 28 | `user_amount_sum_24h` | **Redis on-demand** | `Σ amount(prior 24h) + amount_usd` |
| 29 | `user_seconds_since_last_tx` | **Redis on-demand** | `T − user_last_ts`; NaN nếu chưa có |

Ba nhóm: **request-time** (1–6, 13–19: 13 feature), **static từ Feast** (7–12 + 3 cột hỗ trợ mới: 6 feature + 3 cột), **velocity từ Redis** (20–29: 10 feature).

Lưu ý nhóm static: các giá trị này **bất biến (hoặc gần bất biến) per entity**, nên "dòng materialize cuối" cho giá trị đúng — khác bản chất với nhóm request-time (thuộc tính của *giao dịch*, dòng cuối cho giá trị **sai**). Riêng `account_age_days`/`card_age_days` phụ thuộc thời điểm hiện tại nên chuyển sang derive từ ngày tạo (card ngủ đông 60 ngày sẽ bị sai 60 ngày nếu đọc age cũ).

---

## 4. Kiến trúc

```
                       POST /score  {ids + transaction payload}
                              │
                              ▼
                    ┌──────────────────────┐
                    │   fraud-detection    │ 1. request-time features (pure Python)
                    │        API           │ 2. Feast: 6 static + 3 cột hỗ trợ (1 read)
                    │                      │ 3. Redis Lua "read-then-append":
                    │                      │      đọc windows prior → features
                    │                      │      + ZADD chính giao dịch này (atomic)
                    │                      │ 4. encode → KServe → probability
                    └──────────┬───────────┘
                               │ publish (Kafka, như hiện tại)
                               ▼
                     predictions topic ──► RedisFeaturesRefresher (viết lại)
                                           └─ CHỈ patch trạng thái declined
                                              vào card:declines (khi status về)

     Airflow @daily:  ETL SQL → transaction_features → Feast materialize
     (giữ nguyên — offline training + nhóm static; thêm 3 cột hỗ trợ)
```

**Điểm chốt về write path:** API tự ghi giao dịch hiện tại vào Redis **trong cùng Lua script đọc feature** (1 round-trip, atomic per key). Không dựa vào đường Kafka để ghi state như worker cũ — vì consumer lag vài giây là đủ để fraud burst (các giao dịch cách nhau vài giây) không nhìn thấy nhau. Đường Kafka chỉ còn nhiệm vụ patch `declined` (status chỉ biết sau khi PSP xử lý).

### 4.1 Redis key layout

| Key | Kiểu | Nội dung | Retention |
|---|---|---|---|
| `card:tx:{card_id}` | ZSET | member `"{tx_id}\|{amount}"`, score = epoch giây (float, giữ micro) | evict > 25h trong Lua + `EXPIRE 25h` mỗi lần ghi |
| `card:declines:{card_id}` | ZSET | member `tx_id`, score = epoch | như trên |
| `card:agg:{card_id}` | HASH | `cnt`, `sum`, `sumsq`, `last_ts` | `EXPIRE 90d`, refresh mỗi lần ghi (xem §9 Q3) |
| `card:txmeta:{card_id}` | HASH | `tx_id → "n\|sum\|sumsq\|last_ts"` — snapshot state-prior của từng giao dịch, phục vụ retry-exactness (§4.3) | evict cùng nhịp ZSET + 25h |
| `user:tx:{user_id}` | ZSET | member `"{tx_id}\|{amount}"` | 25h |
| `user:agg:{user_id}` | HASH | `last_ts` | 90d |
| `user:txmeta:{user_id}` | HASH | `tx_id → last_ts` | 25h |

Ước lượng memory: card hoạt động trung bình ~5 tx/24h → ZSET ~300B + agg ~150B ≈ **0.5 KB/card active** → 1M card active ≈ 500 MB. Card ngủ đông chỉ còn agg hash (150B).

Retention 25h (không phải 24h) để cửa sổ 24h không bao giờ bị evict cụt; `last_ts` sống trong agg hash nên gap-since-last-tx không phụ thuộc retention ZSET.

### 4.2 Lua script (per-card; per-user tương tự, gọn hơn)

```lua
-- KEYS: card:tx, card:declines, card:agg
-- ARGV: now_epoch(T), amount, member("txid|amount"), retention_s
local t = tonumber(ARGV[1])

redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, t - tonumber(ARGV[4]))
redis.call('ZREMRANGEBYSCORE', KEYS[2], 0, t - tonumber(ARGV[4]))

-- 1) ĐỌC prior (trước khi append chính mình)
local cnt_1h  = redis.call('ZCOUNT', KEYS[1], t - 3600,  t)
local prior24 = redis.call('ZRANGEBYSCORE', KEYS[1], t - 86400, t)
local sum_24, cnt_24 = 0.0, #prior24
for _, m in ipairs(prior24) do
  sum_24 = sum_24 + tonumber(string.sub(m, string.find(m, '|') + 1))
end
local declines = redis.call('ZCOUNT', KEYS[2], t - 86400, t)
local agg = redis.call('HMGET', KEYS[3], 'cnt', 'sum', 'sumsq', 'last_ts')

-- 2) APPEND chính giao dịch này — idempotent theo member
if not redis.call('ZSCORE', KEYS[1], ARGV[3]) then
  redis.call('ZADD', KEYS[1], t, ARGV[3])
  redis.call('HINCRBY',      KEYS[3], 'cnt',   1)
  redis.call('HINCRBYFLOAT', KEYS[3], 'sum',   ARGV[2])
  redis.call('HINCRBYFLOAT', KEYS[3], 'sumsq', tonumber(ARGV[2])^2)
  redis.call('HSET', KEYS[3], 'last_ts', ARGV[1])
end
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[4]))
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[4]))
redis.call('EXPIRE', KEYS[3], 90 * 86400)

return {cnt_1h, cnt_24, tostring(sum_24), declines,
        agg[1] or 0, agg[2] or '0', agg[3] or '0', agg[4] or false}
```

- **Đọc-trước-ghi-sau** trong một script → không race giữa hai request cùng card; giao dịch sau nhìn thấy giao dịch trước ngay lập tức (không phụ thuộc Kafka lag).

### 4.3 Retry-exactness (bổ sung sau khi làm demo)

Sketch trên chống được double-count aggregates, nhưng **chưa đủ** để retry trả về đúng feature của lần gửi đầu: nếu một giao dịch khác của cùng entity chen vào giữa lần gửi đầu và retry, các feature đọc từ aggregate trọn đời (`gap`, `zscore`, `seq`) không thể suy ngược từ agg nữa. Giải pháp đã chốt (hiện thực trong demo):

- Phép đọc theo cửa sổ thời gian tự exact — bị chặn `score ≤ T` và loại trừ member của chính giao dịch đang chấm.
- Lần gửi đầu snapshot state-prior (`n|sum|sumsq|last_ts`) vào hash `txmeta` (evict cùng nhịp ZSET); retry đọc lại snapshot thay vì suy từ agg.
- Giả định: retry = payload y hệt (`tx_id`, `amount`, `created_at`). Retry sau khi member đã bị evict (>25h) sẽ bị coi là giao dịch mới — ngoài phạm vi retry thực tế (vài giây).

**Reference implementation chạy được:** [scripts/demo_velocity_features.py](../../scripts/demo_velocity_features.py) — Lua đầy đủ + parity oracle mô phỏng ngữ nghĩa SQL, assert từng feature ở từng bước (kể cả cold start, declined patch, cross-card cùng user, retry sau khi có giao dịch chen giữa).
- Redis Cluster: key card* và user* khác hash slot → 2 lệnh `EVALSHA` riêng (2 RTT, ~1–2 ms). Standalone: có thể gộp 1 script.
- Chi phí đọc `ZRANGEBYSCORE` 24h là O(số tx/card/24h) — thực tế < vài chục member; nếu có card POS cực nóng thì cap bằng maxN + fallback (ghi nhận ở §8 rủi ro).

---

## 5. Parity với SQL training — bảng đối chiếu

Nguồn chân lý: [`_BUILD_TEMPLATE` trong feature_etl.py](../../airflow/etl/feature_etl.py). Ký hiệu: `T` = `created_at` giao dịch hiện tại (epoch), `a` = `amount_usd`.

### 5.1 Request-time

| Feature | SQL training | On-demand (Python) |
|---|---|---|
| `hour` | `extract(hour from created_at at time zone 'UTC')` | `created_at.astimezone(UTC).hour` — **bắt buộc UTC**; utils hiện tại của worker dùng `Asia/Ho_Chi_Minh`, tuyệt đối không tái dùng cho các feature này |
| `weekday` | `extract(isodow) − 1` | `created_at.astimezone(UTC).weekday()` (Python `weekday()` sẵn Monday=0, trùng isodow−1) |
| `is_night` | `hour < 6 OR hour >= 23` | y hệt, trên hour UTC |
| `geo_mismatch` | `billing IS DISTINCT FROM ip` | `int(billing != ip)` — Python `None != None → False`, `None != 'US' → True`: trùng ngữ nghĩa `IS DISTINCT FROM` |
| `foreign_ip` | `ip IS DISTINCT FROM user_country` | `int(ip != user_country)` |
| `recipient_differs` | `recipient IS NOT NULL AND recipient IS DISTINCT FROM purchaser` | `int(recipient is not None and recipient != purchaser)` |
| `account_age_days` | `floor(epoch(created_at − u.created_at)/86400)` | `floor((T − user_created_at_epoch)/86400)` |

### 5.2 Velocity

SQL dùng window `RANGE BETWEEN INTERVAL '…' PRECEDING AND CURRENT ROW` — **bao gồm chính dòng hiện tại**. On-demand: Redis chỉ chứa prior → đọc prior rồi **cộng bản thân**.

| Feature | SQL training | On-demand |
|---|---|---|
| `card_tx_count_1h` | `count(*) OVER (… RANGE '1 hour' PRECEDING AND CURRENT ROW)` | `ZCOUNT(card:tx, T−3600, T) + 1` (ZCOUNT inclusive hai đầu = RANGE inclusive) |
| `card_tx_count_24h` | tương tự 24h | `ZCOUNT(…, T−86400, T) + 1` |
| `card_amount_sum_24h` | `sum(amount) OVER` cùng window | `sum_24_prior + a` |
| `card_seconds_since_last_tx` | `epoch(created_at − lag(created_at))` → NULL nếu là dòng đầu | `T − last_ts`; **NaN** nếu agg chưa có `last_ts` |
| `card_amount_zscore` | `(a − avg OVER prior) / nullif(stddev_samp OVER prior, 0)` — `ROWS UNBOUNDED PRECEDING AND 1 PRECEDING` | với `n, S, SS` = agg **prior** (đọc trước khi increment): `mean = S/n`; `var = (SS − S²/n)/(n−1)`; **NaN nếu `n < 2` hoặc `var ≤ 0`**; else `(a − mean)/sqrt(var)`. Khớp: n=0 → avg NULL; n=1 → stddev_samp NULL; std=0 → nullif |
| `card_tx_seq` | `row_number() OVER (PARTITION BY card ORDER BY created_at)` | `cnt_prior + 1` |
| `card_declines_24h` | `greatest(sum(declined) OVER 24h − self_declined, 0)` = **số declined của các giao dịch KHÁC trong 24h** | `ZCOUNT(card:declines, T−86400, T)` — ZSET chỉ chứa prior declined nên khỏi trừ self. Status giao dịch hiện tại chưa biết lúc scoring → nhất quán |
| `user_*` | các window per user tương tự | ZSET/agg per user tương tự |

### 5.3 Cold start — parity tự nhiên, không cần fallback

Giao dịch **đầu tiên** của một card trong training: `count_1h = 1`, `sum_24h = a`, `gap = NULL`, `zscore = NULL` (không có prior), `seq = 1`, `declines = 0`.

Redis rỗng (card mới / state bị flush): `0+1 = 1`, `0+a = a`, `NaN`, `NaN` (n=0), `0+1 = 1`, `0`. **Trùng từng giá trị.** Card mới không cần code riêng; Redis flush toàn bộ = mọi card degrade về ngữ nghĩa "giao dịch đầu tiên" — sai nhưng sai theo cách model đã thấy trong training, và tự hồi phục sau 24h + bootstrap lại agg (§6.3).

### 5.4 Sai lệch chấp nhận được (ghi nhận, không sửa)

- **Peers cùng timestamp:** SQL `RANGE … CURRENT ROW` tính cả các dòng *cùng `created_at` chính xác* (kể cả "tương lai" trong cùng micro giây); on-demand chỉ thấy giao dịch đã đến trước. Xác suất hai giao dịch cùng card trùng đến micro giây ~0.
- **`lag()` với tie:** thứ tự không xác định trong SQL khi trùng timestamp; on-demand dùng `last_ts` lớn nhất. Cùng lý do trên, bỏ qua.
- **zscore từ HINCRBYFLOAT:** sai số float cộng dồn so với `stddev_samp` tính lại từ đầu — ở mức 1e-9, dưới ngưỡng ảnh hưởng split của LightGBM.

---

## 6. Thay đổi theo thành phần

### 6.1 ETL + Feast (nhỏ, backward-compatible)

Thêm **3 cột hỗ trợ** (không phải model feature) vào `transaction_features` + feature view: `user_country`, `user_created_at`, `card_created_at`. ETL đã join sẵn `users`/`cards` trong CTE `ctx` nên chỉ là thêm cột SELECT. Chúng phục vụ API derive `foreign_ip`, `account_age_days`, `card_age_days`.

### 6.2 API (`src/fraud_detection/core/`)

- **`models.py`** — mở rộng `FraudDetectionInputs`: thêm `amount_usd`, `created_at` (optional, default = now UTC), `channel`, `billing_country_code`, `ip_country_code`, `email_purchaser`, `email_recipient`, `merchant_category`, `merchant_risk_level`. Tất cả field mới **Optional** → client cũ không gãy; field thiếu → feature NaN (đúng cơ chế missing hiện có của `build_model_inputs`).
- **Module mới `request_features.py`** — pure functions cho 13 request-time feature (bảng §5.1). Không I/O → unit test trọn vẹn.
- **Module mới `velocity.py`** — Lua scripts + hàm hợp nhất kết quả Redis thành 10 velocity feature (công thức §5.2). Pure phần tính toán, tách khỏi phần gọi Redis.
- **`predict.py`** — lắp ráp: request-time ∪ Feast static ∪ Redis velocity → `build_model_inputs` (giữ nguyên encoder/NaN). Feast chỉ còn đọc 6 static + 3 cột hỗ trợ. Redis lỗi → 10 velocity = NaN + log + metric (fail-open có kiểm soát).
- Env flag `VELOCITY_SOURCE = feast | redis | shadow` phục vụ rollout (§7).

### 6.3 Bootstrap state (script một lần, chạy lại được)

Replay từ Postgres vào Redis lúc cutover (hoặc sau sự cố flush): per card — ZADD các giao dịch 25h gần nhất, declines 25h, agg trọn đời (`cnt = count(*)`, `sum`, `sumsq`, `last_ts = max(created_at)`); per user tương tự. Chỉ là aggregate query trên `application.transactions` — cùng index `(card_id, created_at)` ETL đã tạo.

### 6.4 Worker (`src/workers/features/redis_worker.py`) — viết lại `handle()`

- **Bỏ:** toàn bộ logic schema cũ — `no_transactions_30_days`, `D4`/`D15`, key `user:card:transactions:*` / `user:card:features:*`, timezone HCM.
- **Còn:** consume event mang status cuối → nếu `declined` thì `ZADD card:declines:{card_id} ts tx_id`. Idempotent tự nhiên.
- **Phụ thuộc mở:** topic hiện tại (`PREDICTIONS_TOPIC` do `predict.py` publish) **không mang status** — cần nguồn event auth-result (§9 Q1).

### 6.5 `daily_refresh` CronJob — nghỉ hưu

Eviction đã nằm trong Lua (mỗi lần đọc/ghi) + TTL key → không còn gì để scan hàng ngày. Giữ chart Helm thêm một release để rollback, rồi xoá.

---

## 7. Rollout

| Phase | Nội dung | Điều kiện qua phase |
|---|---|---|
| **1. Request-time** | §6.1 + `models.py` + `request_features.py`; velocity vẫn đọc Feast. Sửa Lỗi 1 (sai input) — diff nhỏ nhất, giá trị lớn nhất | Unit tests + so sánh feature vector trước/sau trên traffic thật (score shift là *kỳ vọng*, vì trước đó input sai) |
| **2. Velocity shadow** | `velocity.py` + Lua + bootstrap; `VELOCITY_SOURCE=shadow`: tính cả hai nguồn, serve Feast, log diff từng feature | 1–2 ngày: diff Redis-vs-Feast đúng pattern kỳ vọng (Redis nhạy hơn trong ngày, hội tụ sau materialize); parity test golden pass |
| **3. Cutover** | `VELOCITY_SOURCE=redis`; worker mới patch declines | Monitor score distribution + fraud-capture proxy; rollback = đổi env về `feast` (instant) |
| **4. Dọn dẹp** | Xoá `daily_refresh`, dead code worker cũ, bỏ 10 velocity khỏi materialize (Feast chỉ còn static); cân nhắc merchant feature view riêng thay vì client gửi | — |

**Parity test (bắt buộc, phase 2):** chọn ~1000 card từ Postgres, replay giao dịch theo thứ tự thời gian qua `velocity.py` + `request_features.py`, so từng feature với dòng tương ứng trong `application.transaction_features` (SQL đã tính). Assert bằng nhau (tolerance 1e-9). Đây là hàng rào chống skew duy nhất có giá trị thực.

**Lưu ý threshold:** `FRAUD_THRESHOLD` hiện tại được vận hành trên phân phối score sinh từ input sai. Sau Phase 1 phân phối score sẽ dịch — cần re-check threshold trên dữ liệu sau-fix (không phải lỗi của thay đổi này, mà là hệ quả của việc sửa lỗi).

---

## 8. Rủi ro & đối sách

| Rủi ro | Đối sách |
|---|---|
| Redis down → mất 10 velocity | NaN + alert (model quen missing); circuit-breaker để không cộng latency chờ timeout |
| Client clock lệch khi gửi `created_at` | clamp `created_at` vào `[now − 5m, now + 5m]`; lệch quá → dùng server now + log |
| Card cực nóng (POS lớn) → `ZRANGEBYSCORE` 24h dài | cap đọc N=1000 member (count vẫn đúng qua ZCOUNT; sum bị cap thì log + metric); thực tế fraud model quan tâm card cá nhân, hiếm chạm cap |
| Double-write khi client retry | idempotent theo member `tx_id\|amount` + snapshot `txmeta` để retry trả đúng feature lần đầu (§4.3) |
| Bootstrap sót → agg thiếu | zscore/seq lệch cho card cũ; chấp nhận degrade + chạy lại bootstrap được bất kỳ lúc nào (idempotent) |
| Score shift sau Phase 1 gây nhiễu vận hành | thông báo trước, có shadow log so sánh, threshold review đi kèm |

---

## 9. Câu hỏi mở (cần chốt khi review)

1. **Nguồn event `declined`:** luồng thật lấy auth-result từ đâu — client gọi lại endpoint feedback, hay topic riêng từ hệ thống thanh toán? Quyết định này định hình worker §6.4. (Trong data giả, `status` có sẵn lúc tạo giao dịch nên mock dễ.)
2. **Client có gửi được merchant payload không?** Thiết kế đang giả định gateway biết `merchant_category`/`merchant_risk_level`. Nếu không, cần merchant feature view keyed `merchant_id` ngay từ Phase 1 thay vì Phase 4.
3. **TTL 90d cho agg hash:** card ngủ đông > 90d mất `cnt/sum/sumsq/last_ts` → giao dịch tiếp theo mang ngữ nghĩa "card mới" (zscore NaN, seq reset). Đánh đổi memory vs đúng-tuyệt-đối. Ý kiến: chấp nhận — "card im 3 tháng bỗng quẹt" vốn là tín hiệu mà các feature khác (gap NaN thay vì số lớn) vẫn phản ánh được một phần; hoặc bỏ TTL nếu memory không phải vấn đề.
4. **`created_at` trong request bắt buộc hay optional-default-now?** Optional tiện client, nhưng bắt buộc thì replay/audit sạch hơn.
