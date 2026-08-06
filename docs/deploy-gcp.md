# Deploy GCP — VM nhỏ + 4 dịch vụ managed

VM: `e2-standard-2` (2 vCPU / 8 GB) + 50 GB disk. Đủ, dùng ~4,6 GB.

Bốn thành phần stateful chuyển sang managed; VM chỉ còn phần compute:

| Local | Trên GCP | Trên VM? |
|---|---|---|
| postgres | Cloud SQL for PostgreSQL | ❌ |
| redis | Memorystore for Redis | ❌ |
| redpanda | Managed Service for Apache Kafka | ❌ |
| minio | Cloud Storage (GCS) | ❌ |
| spark-master + worker | Dataproc Serverless | ❌ |
| **airflow ×3, flink ×2, 3 service streaming** | | ✅ |

VM là **stateless** — xoá rồi dựng lại chỉ cần `.env.gcp`.

File dùng: `data_pipelines/docker-compose.gcp.yml` + `data_pipelines/.env.gcp`
(đã trong `.gitignore`).

---

## 1. Hạ tầng GCP (trước khi chạm VM)

Tạo bằng Terraform hoặc `gcloud`:

- **Cloud SQL** PostgreSQL 18, **bật Private IP**. Tự tạo 4 database:
  `airflow`, `warehouse`, `opsdb`, `feast-registry`.
- **Memorystore** Redis (chỉ có Private IP → VM phải cùng VPC).
- **Managed Kafka** cluster.
- **GCS bucket** cho data lake + một bucket staging cho Dataproc.
- **Service account** cho VM, cần: `roles/storage.objectAdmin`,
  `roles/dataproc.editor`, `roles/managedkafka.client`.
  **Không** cần `roles/cloudsql.client` — kết nối Private IP dùng user/password
  của Postgres, không đi qua IAM.
- **Subnet** bật Private Google Access, cùng VPC với Cloud SQL (Dataproc ghi JDBC
  vào warehouse nên cần đường tới đó).

Không cần key file ở đâu cả — mọi thứ dùng service account gắn trên VM.

### VM cần gì để nối được Cloud SQL

Kết nối trực tiếp Private IP (không dùng Auth Proxy), nên yêu cầu thuần **network**:

| Cần | Vì sao |
|---|---|
| VM **cùng VPC** với instance | Peering của Private Service Access **không transitive** — VM phải ở đúng VPC đã peer, không phải VPC peer với nó |
| Egress TCP 5432 tới dải PSA | Mặc định GCP cho phép mọi egress → thường không phải làm gì |
| **Access scope `cloud-platform`** cho VM | VM tạo với scope hạn chế sẽ bị chặn GCS/Dataproc dù IAM đúng |
| **Cloud NAT** (nếu VM không có external IP) | Không có NAT thì `docker pull`, GCS, Managed Kafka đều không tới được |

**Không** cần: firewall ingress (Cloud SQL nằm trong mạng của Google, ingress rule
của bạn không chi phối), Authorized networks (chỉ áp dụng cho public IP), Cloud SQL
Admin API, hay service account key.

Container không cần cấu hình gì thêm — docker bridge NAT ra `eth0` của VM rồi vào
VPC. Không cần `network_mode: host`.

Cùng region không bắt buộc (VPC là global) nhưng nên cùng để giảm latency: instance
ở `us-central1` thì đặt VM ở `us-central1`.

Test trước khi dựng stack:

```bash
docker run --rm postgres:18 pg_isready -h <private-ip> -p 5432    # accepting connections
```

> **Bẫy hay gặp**: nếu instance bật *Enforce SSL* / *Require SSL*, mọi kết nối sẽ
> fail vì code không set `sslmode`. Hoặc tắt tuỳ chọn đó, hoặc thêm `sslmode=require`
> vào DSN (`include/ops_to_source.py::ops_dsn`, `include/kafka_to_ops.py::ops_dsn`,
> `generator/ops_store.py::pg_dsn`).

---

## 2. Ba topic Kafka

Bản local có `redpanda-init` tạo giúp, trên GCP thì không:

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

**Hai topic `*_rt` bắt buộc `cleanup.policy=compact`.** Sink `upsert-kafka` của
Flink ghi một row cho mỗi `(entity, window)`, mà mỗi giao dịch thuộc 10 window —
không compact thì topic phình vô hạn.

---

## 3. Đẩy code lên GCS

Dataproc không thấy volume của VM, nên job Spark phải nằm trên GCS:

```bash
gcloud storage rsync -r data_pipelines/spark/jobs   gs://<bucket>/code/jobs
gcloud storage cp data_pipelines/shared/feature_windows.py gs://<bucket>/code/
gcloud storage cp postgresql-42.7.4.jar            gs://<bucket>/jars/
```

Hai file cuối **bắt buộc**, không phải tuỳ chọn:

- `feature_windows.py` — `dp3_*` import nó qua `SHARED_DIR`, thứ chỉ tồn tại nhờ
  docker mount ở local. Ship qua `python_file_uris`.
- JDBC jar — bản local lấy bằng `--packages`, Dataproc Serverless không chắc ra
  được Maven. Ship qua `jar_file_uris`.

Lặp lại `rsync` mỗi lần sửa job Spark.

---

## 4. Jar auth cho Flink

Managed Kafka nói SASL/OAUTHBEARER, Flink cần thêm login handler của Google:

**Đã làm sẵn** — `data_pipelines/flink/lib/managed-kafka-auth-login-handler-1.0.6-all.jar`
(8,2 MB) đã có trong repo và đã mount ở cả `flink-jobmanager` lẫn `flink-taskmanager`.

Maven Central **không** publish bản `-all.jar` cho artifact này — chỉ có jar thin
cộng 26 dependency (`google-auth-library-oauth2-http`, `google-api-client`,
`google-http-client`, guava…). Mount 27 jar là không khả thi, và Flink chỉ scan
**top-level** `/opt/flink/lib` nên không thể nhét vào thư mục con. Nên jar này được
shade từ `com.google.cloud.hosted.kafka:managed-kafka-auth-login-handler:1.0.6`,
loại `kafka-clients` (connector Flink đã shade sẵn, thêm bản thứ hai sẽ xung đột).

Build lại khi cần nâng version:

```bash
# pom tối thiểu + maven-shade-plugin, chạy trong container
docker run --rm -v "$PWD:/w" -w /w maven:3.9-eclipse-temurin-17 mvn -B package
```

Jar phải có ở **cả hai** service: consumer Kafka chạy trên TaskManager, nên thiếu ở
đó thì job vẫn submit được rồi chết lúc khởi tạo source.

---

## 5. Điền `.env.gcp`

Thay mọi `<...>`. Những giá trị hay sai:

| Biến | Lấy ở đâu |
|---|---|
| `PG_HOST` | `gcloud sql instances describe <i> --format='value(ipAddresses[0].ipAddress)'` — lấy dòng `type=PRIVATE` |
| `KAFKA_BOOTSTRAP` | `gcloud managed-kafka clusters describe <c> --location=<r>` |
| `REDIS_HOST` | Private IP của Memorystore |
| `LAKE_ROOT` | `gs://<bucket>/` (1 bucket, 4 prefix) **hoặc** `gs://` (4 bucket riêng) |
| `AIRFLOW_JWT_SECRET` | `openssl rand -hex 32` |

`LAKE_ROOT` là **một biến duy nhất** quyết định cả scheme lẫn cách tổ chức bucket.
Cả Spark job lẫn Airflow đều đọc nó (`include/lake.py`), không chỗ nào hardcode
`s3a://` nữa.

Hai công tắc quan trọng đã đặt sẵn: `SPARK_RUNTIME=dataproc` (DAG dùng
`DataprocCreateBatchOperator` thay `docker exec`) và `KAFKA_SECURITY_PROTOCOL=SASL_SSL`.

---

## 6. Dựng stack trên VM

```bash
cd data_pipelines
docker compose -f docker-compose.gcp.yml --env-file .env.gcp build   # image có providers-google + google-auth
docker compose -f docker-compose.gcp.yml --env-file .env.gcp up -d
docker compose -f docker-compose.gcp.yml --env-file .env.gcp ps
```

**Trước đó phải apply DDL một lần.** Cloud SQL chỉ có instance + 4 database rỗng;
schema `ops` / `application` và toàn bộ bảng vẫn phải tạo. Thiếu bước này thì DP0
chết với `schema ops does not exist`.

```bash
set -a; . ./.env.gcp; set +a
docker run --rm -v "$PWD/sql:/sql:ro" -e PGPASSWORD="$AIRFLOW_PASSWORD" postgres:18 sh -c '
  set -e
  psql -h '"$PG_HOST"' -U '"$AIRFLOW_USER"' -d '"$WAREHOUSE_POSTGRES_DB"' \
    -c "CREATE SCHEMA IF NOT EXISTS application"
  for f in /sql/ops/*.sql; do
    psql -h '"$PG_HOST"' -U '"$AIRFLOW_USER"' -d '"$OPS_POSTGRES_DB"' -v ON_ERROR_STOP=1 -f "$f"
  done
  for f in /sql/warehouse/*.sql; do
    psql -h '"$PG_HOST"' -U '"$AIRFLOW_USER"' -d '"$WAREHOUSE_POSTGRES_DB"' -v ON_ERROR_STOP=1 -f "$f"
  done
  echo "=> DDL done"'
```

Idempotent — mọi file `.sql` đều `CREATE ... IF NOT EXISTS`, chạy lại vô hại.

Airflow UI và Flink dashboard chỉ bind `127.0.0.1` (dashboard Flink không có auth).
Xem qua tunnel:

```bash
gcloud compute ssh <vm> -- -L 8090:localhost:8090 -L 8082:localhost:8082
```

---

## 7. Nạp dữ liệu lịch sử (một lần)

Bỏ qua bước này nếu chỉ chạy luồng live.

```bash
# sinh lịch sử -> Cloud SQL
docker compose -f docker-compose.gcp.yml --env-file .env.gcp \
  exec stream-generator python /opt/airflow/repo/data_pipelines/generator/generate_offline.py

# DP0 export -> GCS
docker compose -f docker-compose.gcp.yml --env-file .env.gcp \
  exec airflow-scheduler bash -lc 'cd /opt/airflow/code && \
    python -m include.ops_to_source --from 2025-07-27 --to 2026-07-29'
```

DP1/DP2/DP3 để `ml_pipeline` chạy, hoặc trigger tay từng TaskGroup trong UI.

> Backfill toàn bộ 368 partition sẽ kéo ~262 MB **qua VM** (`copy_file` của
> pyarrow không dùng server-side copy). Chạy được nhưng chậm; nhanh hơn thì dùng
> `gcloud storage cp` trực tiếp giữa hai prefix.

---

## 8. Bật luồng chạy

```bash
# Flink (script tự chèn property SASL vào template SQL theo .env.gcp)
docker compose -f docker-compose.gcp.yml --env-file .env.gcp \
  exec flink-jobmanager /opt/flink/sql/submit.sh

docker compose -f docker-compose.gcp.yml --env-file .env.gcp \
  exec flink-jobmanager /opt/flink/bin/flink list
```

Rồi bật 2 DAG trong UI: `dp0_export_source` (00:05) và `ml_pipeline` (00:15).

---

## 9. Kiểm tra

```bash
# Cloud SQL thông chưa + bảng đã có chưa
set -a; . ./.env.gcp; set +a
docker run --rm -e PGPASSWORD="$AIRFLOW_PASSWORD" postgres:18 \
  psql -h "$PG_HOST" -U "$AIRFLOW_USER" -d "$OPS_POSTGRES_DB" -c '\dt ops.*'

# Kafka auth thông chưa — log service phải in đúng protocol
docker compose -f docker-compose.gcp.yml --env-file .env.gcp logs ops-ingest | tail -5
# mong đợi: kafka=bootstrap...:9092 (SASL_SSL/OAUTHBEARER)

# GCS có file chưa
gcloud storage ls gs://<bucket>/source/transactions/ | head

# Dataproc job có chạy chưa (sau khi ml_pipeline chạy)
gcloud dataproc batches list --region=<region> --limit=5
```

---

## Ngân sách RAM trên 8 GB

| | Trần |
|---|---|
| OS + dockerd | 0,8 GB |
| Airflow ×3 | 1,0 GB |
| Flink JM 1024m + TM 1280m | 2,3 GB |
| 3 service streaming | 0,5 GB |
| **Tổng** | **~4,6 / 8 GB** |

`FLINK_JM_MEMORY` / `FLINK_TM_MEMORY` phải giữ nhỏ: Flink **không tự co** theo RAM
máy, default là 1600m + 1728m = 3,3 GB dù state thật chỉ ~1,5 MB.

**Không train model trên VM này** — LightGBM 240k × 60 cần ~2 GB và ăn hết CPU.

---

## Lỗi thường gặp

| Triệu chứng | Nguyên nhân |
|---|---|
| `schema ops does not exist` | chưa apply DDL (bước 6) |
| `connection refused` tới Private IP | VM khác VPC với instance, hoặc chưa bật Private IP |
| `SSL connection is required` | instance bật Enforce SSL — xem ghi chú ở bước 1 |
| Flink job RUNNING nhưng topic sink rỗng | thiếu jar `managed-kafka-auth-login-handler` |
| Dataproc batch fail `ModuleNotFoundError: feature_windows` | chưa upload qua `DATAPROC_PYFILES` |
| Dataproc batch fail `No suitable driver` | chưa upload JDBC jar qua `DATAPROC_JDBC_JAR` |
| Dataproc batch fail `KeyError: 'MINIO_ROOT_USER'` | `LAKE_ROOT` không bắt đầu bằng `gs://` → job đi nhánh S3 |
| `ops-ingest` không nhận message | topic chưa tạo, hoặc SA thiếu `roles/managedkafka.client` |
| Batch ID bị từ chối | `batch_id` chỉ nhận `[a-z0-9-]`; DAG đã tự đổi `_` → `-` |
| Topic `*_rt` phình to | quên `cleanup.policy=compact` |

---

## Điểm chưa làm

- **Terraform** cho GCS/Dataproc/IAM — chưa viết.
- **Secret Manager**: `spark.dataproc.driverEnv.PG_PASSWORD` đọc được bằng
  `gcloud dataproc batches describe`. Với môi trường thật phải chuyển sang Secret
  Manager.
- **DP1 dùng server-side copy** của GCS thay vì kéo bytes qua VM.
