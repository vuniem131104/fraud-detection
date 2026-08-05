# Deploy local — từ số 0 đến hệ đang chạy

Máy tham chiếu: 12 vCPU / 14 GB. Tối thiểu 4 vCPU / 8 GB.

Cần có: `docker`, `docker compose`, `uv`, và file `data_pipelines/.env` (secret
Postgres/MinIO — không có trong git).

Toàn bộ quy trình có hai phần: **Giai đoạn 1** nạp lịch sử một lần rồi train,
**Giai đoạn 2** bật luồng chạy hằng ngày.

---

## 0. Dựng hạ tầng

```bash
cd data_pipelines
docker compose down -v          # CHỈ khi muốn làm lại từ đầu (xoá hết dữ liệu)
docker compose up -d
docker compose ps
```

Ba container init phải ở trạng thái `Exited (0)`: `postgres-init` (tạo 3 DB +
apply DDL), `minio-init` (tạo 4 bucket), `redpanda-init` (tạo 3 topic). Không có
bước thủ công nào — nếu có thì `down -v` sẽ làm mất.

Nếu `airflow-dag-processor` restart liên tục với `FileNotFoundError: .../logs/...`:

```bash
docker compose exec -u 0 airflow-scheduler chown -R 1000:0 /opt/airflow/logs
docker compose restart airflow-dag-processor
```

Thư mục `airflow/logs` do root tạo nhưng container chạy uid 1000.

---

## 1. Sinh dữ liệu lịch sử → Postgres

```bash
cd ..    # về gốc repo
uv run python data_pipelines/generator/generate_offline.py
```

~2 phút. Ghi vào `ops.transactions` (302.958 dòng, 368 ngày), `ops.{users,cards,
merchants,devices}`, và `application.labels`. Cửa sổ lịch sử lấy từ
`generator/generator_config.yaml`: **27/07/2025 → 29/07/2026**.

Nó in ra một quality report — kiểm nhanh 4 con số: duplicate ~1%, US ~80%, fraud
~0,5%, và `merchant/10 phút max` phải **> 1** (nếu bằng 1 thì feature real-time
sẽ vô dụng).

Cuối output có dòng `Bước tiếp: ... --from X --to Y` — dùng đúng ngày đó ở bước sau.

---

## 2. DP0 — team data export ra MinIO

```bash
cd data_pipelines
docker compose exec -e MINIO_ENDPOINT=minio:9000 airflow-scheduler \
  bash -lc 'cd /opt/airflow/code && python -m include.ops_to_source \
    --from 2025-07-27 --to 2026-07-29'
```

Một query cho cả năm, ~1 phút. Ra `s3://source/` với 368 partition transactions +
4 snapshot dim. Đây là **ranh giới giữa hai team**: từ đây trở đi team ML chỉ thấy file.

---

## 3. DP1 — copy source → raw (Bronze)

```bash
docker compose exec -T airflow-scheduler python - <<'PY'
import sys; sys.path.insert(0, "/opt/airflow/code")
from include.minio_io import get_lake_fs, copy_dataset
fs = get_lake_fs()
for d in ["transactions", "users", "cards", "merchants", "devices"]:
    print(d, copy_dataset(fs, "source", "raw", d), "file")
PY
```

Bronze giữ **data thô**: không dedup, không sửa kiểu. Duplicate và schema khác
nhau giữa các partition được giữ nguyên để DP2 chứng minh nó xử lý được.

---

## 4. DP2 + DP3 — Spark

`spark-master` **không có `env_file`**, nên cred phải truyền bằng `-e`. Thiếu là
lỗi `KeyError: 'MINIO_ROOT_USER'` ngay lúc khởi tạo SparkSession.

```bash
set -a; . ./.env; set +a
SPARK() {
  docker compose exec -T \
    -e MINIO_ROOT_USER="$MINIO_ROOT_USER" -e MINIO_ROOT_PASSWORD="$MINIO_ROOT_PASSWORD" \
    -e PG_USER="$AIRFLOW_USER" -e PG_PASSWORD="$AIRFLOW_PASSWORD" \
    spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.4 \
    --conf spark.jars.ivy=/tmp/.ivy2 "$@"
}

SPARK /opt/spark/jobs/dp2_bronze_to_silver.py --date all
SPARK /opt/spark/jobs/dp2_silver_to_gold.py --stage fact --date all
SPARK /opt/spark/jobs/dp2_silver_to_gold.py --stage dims
SPARK /opt/spark/jobs/dp3_gold_to_features.py --date all
SPARK /opt/spark/jobs/dp3_training_features.py --lookback-days 400
```

Kiểm output từng bước:

| Job | Phải thấy |
|---|---|
| `bronze_to_silver` | `rows 302,958 -> 299,959 (removed 2,999 duplicates)` |
| `silver_to_gold --stage dims` | `initial load` cho 4 dim (SCD2 version đầu) |
| `dp3_gold_to_features` | 4 bảng `feat_*`, as-of `2026-07-29` |
| `dp3_training_features` | `feat_training: 299,959 dòng, 55 cột` |

`--lookback-days 400` phải **lớn hơn** 368 ngày lịch sử.

---

## 5. Feast → Redis

```bash
docker compose exec airflow-scheduler bash -lc \
  'cd /opt/airflow/feature_store && feast apply'

docker compose exec airflow-scheduler bash -lc \
  'cd /opt/airflow/feature_store && feast materialize 2025-07-27T00:00:00 \
     "$(date -u +%Y-%m-%dT%H:%M:%S)" \
     --views card_features --views user_features \
     --views merchant_features --views device_features'
```

**Phải liệt kê đúng 4 view.** `feast materialize` trơn sẽ chạm cả hai view của
Flink và ghi giá trị batch (chậm tới 24h) lên giá trị real-time.

Kiểm tra: `docker compose exec redis redis-cli DBSIZE` phải bằng tổng số dòng 4
bảng `feat_*` — hiện tại là `27.494 + 25.000 + 30.000 + 1.500 = 83.994`.

---

## 6. Train model

MLflow là stack riêng ở `infra/docker/`, cred lấy từ `.env` ở gốc repo:

```bash
cd ..
docker compose --env-file .env -f infra/docker/docker-compose.yml \
  up -d --build mlflow-postgres mlflow
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:5000/health   # 200
```

Rồi mở `scripts/training/model_training.ipynb`, hoặc chạy headless:

```bash
cd scripts/training
uv run --with nbconvert --with ipykernel -- jupyter nbconvert \
  --to notebook --execute --inplace --ExecutePreprocessor.timeout=3600 \
  model_training.ipynb
```

Notebook đọc `feat_training ⋈ labels` (một query, **không** cần Redis hay Flink),
tính 22 feature on-demand bằng đúng công thức của ODFV, train LightGBM và đăng ký
model lên MLflow. Artifact ra `models/`.

Kết quả tham chiếu: PR-AUC ~0,82 / precision@0,5% ~96% / 60 feature.

---

## 7. Giai đoạn 2 — bật luồng hằng ngày

```bash
cd data_pipelines

# Flink: script tự thay biến Kafka vào template SQL rồi submit
docker compose exec flink-jobmanager /opt/flink/sql/submit.sh

# xác nhận
docker compose exec flink-jobmanager /opt/flink/bin/flink list
```

`sql-client.sh` submit job rồi **thoát** — job sống trong cluster, không cần
`nohup`. Đừng chạy hai lần: sẽ có hai job đọc trùng topic.

Ba service streaming đã tự chạy (`restart: unless-stopped`):

```bash
docker compose ps stream-generator ops-ingest feature-bridge
```

Cuối cùng bật 2 DAG ở http://localhost:8090 (login trong `.env`):

| DAG | Giờ | Việc |
|---|---|---|
| `dp0_export_source` | 00:05 | `ops.*` → MinIO `source` |
| `ml_pipeline` | 00:15 | DP1 → DP2 → DP3 → materialize |

Nên bật **trước 00:05** để sáng mai có một vòng chạy đầy đủ đầu tiên.

---

## 8. Kiểm tra

```bash
# test chống lệch định nghĩa velocity (bắt buộc chạy khi sửa cửa sổ)
REDIS_HOST=localhost REDIS_PORT=6380 uv run pytest tests/test_velocity_parity.py -v

# serving đọc được feature chưa
docker compose exec -T airflow-scheduler python -c "
from feast import FeatureStore
s = FeatureStore(repo_path='/opt/airflow/feature_store')
print(s.get_online_features(
    features=['card_features:card_tx_count_90d'],
    entity_rows=[{'card_id': '<id-thật-trong-feat_card>'}]).to_dict())"

# Flink đã ra kết quả chưa
docker compose exec redpanda rpk topic consume merchant_rt_10min -n 3
```

---

## Cổng

| Service | URL |
|---|---|
| Airflow | http://localhost:8090 |
| MinIO console | http://localhost:9001 |
| Spark master | http://localhost:8080 |
| Flink dashboard | http://localhost:8082 |
| Redpanda console | http://localhost:8085 |
| MLflow | http://localhost:5000 |
| Redis | `localhost:6380` (không phải 6379) |

---

## Lỗi thường gặp

| Triệu chứng | Nguyên nhân |
|---|---|
| `KeyError: 'MINIO_ROOT_USER'` khi chạy Spark | thiếu `-e` — `spark-master` không có `env_file` |
| `ModuleNotFoundError: feature_windows` / `feature_views` | `PYTHONPATH` thiếu `shared` + `feature_store` (đã set trong compose; kiểm nếu tự chạy bằng tay) |
| `dag-processor` restart liên tục | `airflow/logs` không cho uid 1000 ghi — xem bước 0 |
| `stream-generator` báo `Cannot choose from an empty sequence` | `ops.*` còn rỗng, chạy bước 1 trước |
| `feature-bridge` restart | chưa submit job Flink, hoặc thiếu `PYTHONPATH` |
| Airflow/Flink UI "unhealthy" nhưng vào được | healthcheck sai endpoint (đã sửa trong compose) |
| Giá trị real-time luôn bằng 0 | đúng hành vi nếu entity im lặng > 420s (merchant) / 660s (device) |
