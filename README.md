# Fraud detection — data pipeline trên GCP

Pipeline dữ liệu cho hệ phát hiện gian lận thẻ: sinh giao dịch live → Kafka →
Cloud SQL → data lake 4 tầng trên GCS → feature store (offline Postgres + online
Redis), cộng một job Flink tính feature real-time.

Nhánh này **chỉ chạy trên GCP**. Không có đường local: mọi thành phần stateful đều
là dịch vụ managed, VM chỉ chạy phần compute.

---

## 1. Kiến trúc

```
                    stream-generator (VM)
                            │
                            ▼
              Managed Kafka  topic: transactions
                   │                    │
       ops-ingest (VM)                Flink (VM)
                   │                    │  HOP 10 phút / 1 giờ
                   ▼                    ▼
         Cloud SQL: opsdb        Kafka: merchant_rt_10min
         ops.transactions              device_rt_1h
                   │                    │
          DP0 export (Airflow)   feature-bridge (VM)
                   │                    │
                   ▼                    ▼
    GCS  source → raw → staging → curated      Memorystore Redis
              (DP1)   (DP2 Spark local trên VM)   (online store)
                            │                         ▲
                            ▼                         │
              Cloud SQL: warehouse.application.feat_* ─┘
                     (DP3)      feast materialize
```

`ops.transactions` là **system of record duy nhất**. Cả batch lẫn streaming đều
tính trên cùng một tập giao dịch nên không thể phân kỳ.

### Chạy ở đâu

| Thành phần | Nơi chạy |
|---|---|
| Airflow (webserver, scheduler, dag-processor) | VM |
| Flink (jobmanager + taskmanager) | VM |
| stream-generator, ops-ingest, feature-bridge | VM |
| Postgres (opsdb, warehouse, airflow, feast-registry) | **Cloud SQL** (Private IP) |
| Online feature store | **Memorystore Redis** |
| Kafka | **Managed Service for Apache Kafka** |
| Data lake (4 tầng medallion) | **Cloud Storage** |
| Spark (DP2, DP3) | **VM**, `spark-submit --master local[2]` trong container Airflow |

VM là **stateless**: volume duy nhất có state là checkpoint Flink (~1,5 MB). Xoá
VM rồi dựng lại chỉ cần `.env`.

### Ba tầng feature

| Tầng | Cửa sổ | Tính lúc train | Tính lúc serve |
|---|---|---|---|
| Batch | 7d / 30d / 90d | Spark (DP3) | Redis, qua `feast materialize` |
| Flink | 10 phút / 1 giờ | Spark (DP3) | Flink → bridge → Redis |
| Đồng bộ | 5 phút | Spark (DP3) | Redis sorted set, trong đường score |
| On-demand | — | cùng một hàm Python | ODFV lúc request |

Spark tính **tất cả** cho offline; mỗi nhóm có **đúng một** người ghi online.

---

## 2. Cấu trúc repo

```
airflow/dags/          dp0_export_source.py (00:05), ml_pipeline.py (00:15)
airflow/include/       lake.py, lake_io.py, kafka_conf.py,
                       ops_to_source.py (DP0), kafka_to_ops.py, feature_bridge.py
spark/jobs/            dp2_bronze_to_silver, dp2_silver_to_gold,
                       dp3_gold_to_features, dp3_training_features
flink/sql/             realtime_features.sql (template) + submit.sh
flink/lib/             3 jar: connector kafka (THIN) + kafka-clients + auth handler
generator/             generate_offline.py, generate_stream.py, ops_store.py,
                       generate_fake_data.py, generator_config.yaml
shared/                feature_windows.py — độ dài cửa sổ + ngưỡng độ tươi
sql/ops/, sql/warehouse/   DDL
feature_store/         Feast repo (entity, data source, feature view, ODFV)
docker-compose.yml     10 service trên VM
.env                   secret + endpoint (KHÔNG commit)
```

`shared/feature_windows.py` là nguồn duy nhất cho độ dài cửa sổ. Spark, Feast,
bridge và DAG đều đọc từ đó — sửa một chỗ, không lệch định nghĩa.

---

## 3. Chuẩn bị hạ tầng GCP

### 3.1 Dịch vụ

- **Cloud SQL** PostgreSQL, **bật Private IP**, tự tạo 4 database: `airflow`,
  `warehouse`, `opsdb`, `feast-registry`.
- **Memorystore** Redis (Private IP, cùng VPC).
- **Managed Kafka** cluster.
- **GCS** bucket cho data lake.
- **Service account** cho VM: `roles/storage.objectAdmin`, `roles/managedkafka.client`.
  Không cần `roles/cloudsql.client` — Private IP dùng user/password của Postgres.

### 3.2 Ba topic Kafka

```bash
gcloud managed-kafka topics create transactions \
  --cluster=<cluster> --location=<region> --partitions=3

gcloud managed-kafka topics create merchant_rt_10min \
  --cluster=<cluster> --location=<region> --partitions=1 \
  --configs=cleanup.policy=compact

gcloud managed-kafka topics create device_rt_1h \
  --cluster=<cluster> --location=<region> --partitions=1 \
  --configs=cleanup.policy=compact
```

**Hai topic `*_rt` bắt buộc `cleanup.policy=compact`**: sink `upsert-kafka` của
Flink ghi một row cho mỗi `(entity, window)` mà mỗi giao dịch thuộc 10 window —
không compact thì topic phình vô hạn.

### 3.3 VM

```bash
gcloud compute instances create fraud-detection \
  --zone=us-central1-a \
  --machine-type=e2-standard-4 \
  --subnet=default \
  --service-account=<sa>@<project>.iam.gserviceaccount.com \
  --scopes=https://www.googleapis.com/auth/cloud-platform \
  --image-family=ubuntu-2404-lts-amd64 --image-project=ubuntu-os-cloud \
  --boot-disk-size=50GB --boot-disk-type=pd-balanced
```

`--scopes=cloud-platform` là **bắt buộc**. Scope mặc định gồm
`devstorage.read_only` → DP0 không ghi được GCS, và Spark/Kafka bị chặn **dù
IAM đúng hoàn toàn**. Scope là lớp chặn nằm trước IAM.

Yêu cầu network để nối Cloud SQL qua Private IP:

| Cần | Ghi chú |
|---|---|
| VM **cùng VPC** với instance | Peering của Private Service Access không transitive |
| Egress TCP 5432 | GCP mặc định cho phép mọi egress |
| Cloud NAT (nếu VM không có external IP) | Không có thì `docker pull`/GCS/Kafka không tới được |

Không cần firewall ingress, Authorized networks, hay service account key.

Cài Docker:

```bash
gcloud compute ssh fraud-detection --zone=us-central1-a
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER && exit    # logout/login để group có hiệu lực
sudo apt-get update && sudo apt-get install -y git
```

### 3.4 Code Spark

Không phải deploy gì. `docker-compose.yml` mount `./spark/jobs` vào container
Airflow, job chạy bằng `spark-submit --master local[2]` ngay tại đó — sửa job
xong là lần chạy sau ăn ngay.

Hai thứ bắt buộc phải có sẵn trong image (xem `Dockerfile`):

- **gcs-connector** — job đọc/ghi thẳng `gs://`, Hadoop không tự hiểu scheme này.
- **JDBC Postgres** — `dp3_*` ghi `feat_*` vào Cloud SQL.

`shared/feature_windows.py` được ship cho job qua `--py-files`.

### 3.5 Áp DDL lên Cloud SQL

Cloud SQL chỉ có instance + 4 database rỗng; schema và bảng vẫn phải tạo. Thiếu
bước này thì DP0 chết với `schema ops does not exist`.

```bash
set -a; . ./.env; set +a
docker run --rm -v "$PWD/sql:/sql:ro" -e PGPASSWORD="$AIRFLOW_PASSWORD" postgres:18 sh -c '
  set -e
  psql -h '"$PG_HOST"' -U '"$AIRFLOW_USER"' -d '"$WAREHOUSE_POSTGRES_DB"' \
    -c "CREATE SCHEMA IF NOT EXISTS application"
  for f in /sql/ops/*.sql; do
    psql -h '"$PG_HOST"' -U '"$AIRFLOW_USER"' -d '"$OPS_POSTGRES_DB"' -v ON_ERROR_STOP=1 -f "$f"
  done
  for f in /sql/warehouse/*.sql; do
    psql -h '"$PG_HOST"' -U '"$AIRFLOW_USER"' -d '"$WAREHOUSE_POSTGRES_DB"' -v ON_ERROR_STOP=1 -f "$f"
  done'
```

Idempotent, chạy lại vô hại.

---

## 4. Cấu hình `.env`

| Biến | Lấy ở đâu |
|---|---|
| `PG_HOST` | `gcloud sql instances describe <i> --format='value(ipAddresses[0].ipAddress)'` (dòng `type=PRIVATE`) |
| `REDIS_HOST` | Private IP của Memorystore |
| `KAFKA_BOOTSTRAP` | `gcloud managed-kafka clusters describe <c> --location=<r>` |
| `LAKE_ROOT` | `gs://<bucket>/` (1 bucket, 4 prefix) hoặc `gs://` (4 bucket riêng) |
| `AIRFLOW_JWT_SECRET` | `openssl rand -hex 32` |
| `SPARK_MASTER`, `SPARK_DRIVER_MEMORY` | `local[2]` / `3g` — mặc định đã hợp e2-standard-4 |

`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` và `FEAST_REGISTRY_PATH` là URL nên **phải
ghi lại Private IP**, không đọc được `PG_HOST`.

`LAKE_ROOT` là biến duy nhất quyết định layout data lake. Bốn tầng
(`source`/`raw`/`staging`/`curated`) đứng ngay sau nó, nên `gs://<bucket>/` biến
chúng thành prefix, còn `gs://` biến chúng thành tên bucket.

---

## 5. Kiểm kết nối trước khi deploy

Chạy trên VM sau khi build image, **trước** `docker compose up`:

```bash
docker compose build
docker run --rm --env-file .env -v "$PWD:/repo" -w /repo \
  --entrypoint python fraud-detection/airflow:3.2.2 check_connections.py
```

Năm check, mỗi cái độc lập nên một lần chạy thấy hết vấn đề. Exit code = số check
FAIL. Chạy riêng: `check_connections.py --only postgres,kafka`.

| Check | Kiểm gì |
|---|---|
| `sa` | SA đang gắn + có scope `cloud-platform` |
| `postgres` | TCP → 4 database → schema `ops`/`application` đủ bảng (DDL đã apply chưa) |
| `redis` | TCP → PING → ghi/đọc/xoá một key (chứng minh có quyền WRITE) |
| `kafka` | TCP → metadata (token OAUTHBEARER) → 3 topic → `cleanup.policy=compact` |
| `gcs` | list → ghi/đọc/xoá một object trong `LAKE_ROOT` |

Mỗi FAIL in kèm gợi ý xử lý. Hai check dễ bị bỏ qua nhưng hay fail nhất:

- **`gcs` ghi** — scope mặc định của VM có `devstorage.read_only`, nên list được mà
  ghi thì fail, và DP0 sẽ chết đúng ở bước ghi sau khi mọi thứ khác trông như ổn.
- **`kafka` metadata** — thiếu scope hoặc `roles/managedkafka.client` thì treo ở
  bước lấy token, không phải ở TCP.

---

## 6. Dựng stack

```bash
git clone <repo> && cd fraud-detection
# copy .env đã điền vào gốc repo
docker compose build
docker compose up -d
docker compose ps
```

Mọi UI **chỉ bind `127.0.0.1`** (Flink dashboard và Spark UI đều không có auth).
Xem qua tunnel:

```bash
gcloud compute ssh fraud-detection --zone=us-central1-a \
  -- -L 8090:localhost:8090 -L 8082:localhost:8082 \
     -L 18080:localhost:18080 -L 4040:localhost:4040 -L 4041:localhost:4041
```

| Cổng | UI |
|---|---|
| 8090 | Airflow |
| 8082 | Flink dashboard |
| 18080 | **Spark History Server** — mọi job đã chạy xong |
| 4040 / 4041 | Spark UI của job **đang** chạy (task Spark thứ hai nhảy sang 4041) |

#### Soi skew trên Spark UI

DP2/DP3 xong trong vài phút nên 4040 hầu như không kịp mở — dùng 18080. Vào
`ml_pipeline` → app tương ứng → tab **Stages** → stage có `Window [merchant_id]`
→ bảng **Summary Metrics**:

- `Duration` cột **max / median** — trên 5x là skew thật
- `Shuffle Read Size / Records` max/median — xác nhận lệch do dữ liệu, không do máy
- `Spill (memory/disk)` khác 0 — buffer window không vừa RAM

Skew này được **tiêm cố ý** bằng `dirty.skew.hot_merchant_share` trong
`generator/generator_config.yaml` (25% giao dịch dồn về một merchant). Xem
`spark/jobs/dp3_training_features.py` để biết vì sao merchant là key đau nhất:
`merchant_distinct_cards_30d` dùng `size(collect_set(...))` trên rolling window,
chi phí bậc hai theo số dòng của key.

### 5.1 Nạp dữ liệu lịch sử (một lần, tuỳ chọn)

Bỏ qua nếu chỉ chạy luồng live.

```bash
# sinh 100k giao dịch (08/04/2026 → 08/08/2026, 123 ngày) vào Cloud SQL — ~2 phút
 docker compose run --rm --entrypoint python stream-generator /opt/airflow/repo/generator/generate_offline.py

# export toàn bộ cửa sổ lịch sử ra GCS source.
# --to là INCLUSIVE: 2026-08-08 là partition cuối mà generator sinh ra
# (end_date trong generator_config.yaml là exclusive). Muốn kèm cả ngày live
# hôm nay thì để --to là ngày hiện tại, partition đó sẽ là dữ liệu dở dang.
docker compose exec airflow-scheduler bash -lc \
  'cd /opt/airflow/code && python -m include.ops_to_source \
     --from 2026-04-08 --to 2026-08-08'
```

Rồi trigger `ml_pipeline` trong UI để chạy DP1 → DP2 → DP3 → materialize.

### 5.2 Submit job Flink

```bash
docker compose exec flink-jobmanager /opt/flink/sql/submit.sh
docker compose exec flink-jobmanager /opt/flink/bin/flink list
```

`submit.sh` thay `KAFKA_BOOTSTRAP` + property SASL vào `realtime_features.sql`
(là template, vì Flink SQL không nội suy biến môi trường) rồi gọi `sql-client`.

`sql-client -f` submit job rồi **thoát** — job sống trong cluster, không cần
`nohup`. Đừng chạy hai lần: sẽ có hai job đọc trùng topic.

### 5.3 Bật 2 DAG

Trong UI (`http://localhost:8090` qua tunnel):

| DAG | Giờ | Việc |
|---|---|---|
| `dp0_export_source` | 00:05 | `ops.*` → GCS `source` |
| `ml_pipeline` | 00:15 | DP1 → DP2 → DP3 → validate → materialize |

Nên bật **trước 00:05** để sáng hôm sau có một vòng chạy đầy đủ.

---

## 6. Kiểm tra

```bash
# SA + scope đúng chưa
curl -s -H 'Metadata-Flavor: Google' \
  http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email

# Cloud SQL thông chưa
set -a; . ./.env; set +a
docker run --rm postgres:18 pg_isready -h "$PG_HOST" -p 5432

# Kafka auth thông chưa — log phải in SASL_SSL/OAUTHBEARER
docker compose logs ops-ingest | tail -5

# GCS có file chưa
gcloud storage ls "$LAKE_ROOT"source/transactions/ | head

# Flink đã ra kết quả chưa
gcloud managed-kafka topics describe merchant_rt_10min --cluster=<c> --location=<r>

# Log Spark (chạy trong chính task Airflow)
docker compose logs -f airflow-scheduler | grep spark-submit
```

---

## 7. Vài quyết định thiết kế đáng biết

**Vì sao Flink ở lại VM thay vì dùng managed.** GCP không có managed Flink
first-party (Dataflow chạy Beam → phải viết lại `realtime_features.sql`; Dataproc
có Flink nhưng là cluster thường trú, đắt hơn cả VM). Tải thật ~813 giao dịch/ngày
= 0,0094 msg/s, đo được 0,97% CPU — mọi managed service đều có sàn chi phí lớn hơn
2,3 GB RAM ở đây.

**Vì sao Flink ghi ra Kafka rồi mới vào Redis.** Format khoá Redis là chi tiết nội
bộ của Feast, mà API ghi của Feast là Python — Flink SQL không gọi được. Nên phải
có một chỗ nối; Kafka cho thêm độ bền (bridge chết thì kết quả nằm chờ) và khả
năng debug (biết Flink tính đúng hay bridge ghi sai).

**Flink consume ngay nhưng KHÔNG push ngay.** Output chỉ phát ra khi watermark
vượt `window_end`, mà watermark = `max event-time − 90s` và chỉ tiến khi có message
mới tới. Ở nhịp 1 message/~105 giây, một giao dịch xuất hiện trong topic kết quả
sau 2–4 phút. Đó là lý do có `RT_STALE_GRACE_S`.

**Hai bảng `feat_merchant_rt` / `feat_device_rt` cố ý để RỖNG.** Feast bắt mọi
`PushSource` phải khai `batch_source`; trỏ vào bảng có dữ liệu thì một lệnh
`feast materialize` lỡ tay sẽ ghi giá trị batch (chậm tới 24h) lên giá trị Flink
vừa đẩy. Để rỗng thì lỡ chạm cũng đọc 0 dòng → no-op.

**`feast materialize` phải liệt kê đúng 4 view batch.** Lệnh trơn sẽ chạm cả hai
view của Flink. Xem `BATCH_VIEWS` / `PUSH_VIEWS` trong `shared/feature_windows.py`.

**Airflow 3 dùng `CronTriggerTimetable`**: `logical_date` = chính thời điểm cron
bắn, nên `logical_date - 1 day` cho ra ngày hôm trước. `data_interval_start` ở đây
bằng luôn `logical_date` → dùng nó sẽ sai ngày.

---

## 8. Lỗi thường gặp

| Triệu chứng | Nguyên nhân |
|---|---|
| `schema ops does not exist` | chưa áp DDL (§3.5) |
| `connection refused` tới Private IP | VM khác VPC với instance, hoặc chưa bật Private IP |
| `SSL connection is required` | instance bật Enforce SSL — code không set `sslmode`, phải tắt hoặc sửa 3 hàm DSN |
| `pg_hba.conf rejects connection ... no encryption` (task `materialize`) | `feature_store.yaml` khai `sslmode: disable` — đổi thành `require`. Các DSN khác ăn default `prefer` nên chỉ Feast vỡ |
| `FeatureViewNotFoundException` lúc materialize | chưa `feast apply` (bước tay, không DAG nào chạy) — xem §5.2 |
| DP0 không ghi được GCS | VM thiếu scope `cloud-platform` |
| `submit.sh` báo `thiếu KAFKA_BOOTSTRAP` | container `flink-jobmanager` chưa nạp env mới: `docker compose up -d flink-jobmanager` |
| `NoClassDefFoundError: org/apache/kafka/common/security/auth/AuthenticateCallbackHandler` | đang dùng uber jar `flink-sql-connector-kafka` (relocate kafka-clients) cùng auth handler (implement package GỐC). Phải là connector **thin** + `kafka-clients` không shade, và **bỏ** uber jar khỏi `/opt/flink/lib` |
| Flink job RUNNING nhưng topic sink rỗng | thiếu jar `managed-kafka-auth-login-handler` ở TaskManager |
| Spark fail `ModuleNotFoundError: feature_windows` | `./shared` chưa được mount vào container |
| Spark fail `No suitable driver` | image cũ, chưa có `/opt/spark-jars/postgresql.jar` — build lại |
| Spark fail `KeyError: 'LAKE_ROOT'` | thiếu `LAKE_ROOT` trong `.env` |
| `ModuleNotFoundError: feature_windows` / `feature_views` trong Airflow | `PYTHONPATH` thiếu `shared` + `feature_store` |
| `dag-processor` restart liên tục | `airflow/logs` không cho uid 1000 ghi: `docker compose exec -u 0 airflow-scheduler chown -R 1000:0 /opt/airflow/logs` |
| Giá trị real-time luôn 0 | đúng hành vi nếu entity im lặng > 420s (merchant) / 660s (device) |
| Topic `*_rt` phình to | quên `cleanup.policy=compact` |

---

## 9. Chưa làm

- **Terraform** cho GCS/IAM/VM.
- **Secret Manager**: mật khẩu Postgres hiện nằm trong `.env` và đi vào job Spark
  qua env của container.
- **DP1 dùng server-side copy** của GCS thay vì kéo bytes qua VM (`copy_file` của
  pyarrow tải xuống rồi đẩy lên lại — không sao ở nhịp 1 file/ngày, nhưng backfill
  123 partition sẽ kéo lại toàn bộ ~25 MB của source vô ích).
- **Label delay thật** (chargeback 30–120 ngày): hiện giả định label tức thời.
