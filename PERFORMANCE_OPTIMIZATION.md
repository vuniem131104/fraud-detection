# Fraud Detection API Performance Optimization

## Tong quan

Tai lieu nay tom tat cac thay doi da ap dung de giam latency cho endpoint
`POST /score` va cach chay lai benchmark bang Locust.

Ngay benchmark: `2026-05-31`.

## Van de ban dau

Bao cao ban dau trong `report.md` cho thay API bi queue request khi chay
`50 RPS`:

| Chi so | Gia tri ban dau |
|---|---:|
| Tong request | 500 |
| Thanh cong | 495 |
| Loi | 5 |
| Throughput hoan tat | 15.71 RPS |
| Latency trung binh | 15.67 s |
| p95 | 22.95 s |
| p99 | 23.22 s |

Log Redis tang tu vai tram ms len gan mot giay khong phai do Redis server
qua tai. Nguyen nhan chinh la API chi chay mot worker va event loop bi chiem
boi phan xu ly pandas dong bo.

## Cac thay doi da ap dung

### 1. Bat multi-worker API thuc su

Lenh Docker cu dung dong thoi:

```bash
uvicorn ... --workers 5 --reload
```

Uvicorn bo qua `--workers` khi bat `--reload`, vi vay API thuc te chi co mot
worker. Da bo `--reload`, them `--no-access-log` cho benchmark va cho phep cau
hinh worker qua bien moi truong:

```bash
API_WORKERS=5
```

File lien quan:

- `infra/docker/Dockerfile.api`
- `infra/docker/docker-compose.yml`

### 2. Tai su dung HTTP client khi goi KServe

Truoc day moi request tao va dong mot `httpx.AsyncClient`. Cach nay lam mat
HTTP keep-alive va tao them chi phi ket noi.

Da chuyen sang mot `httpx.AsyncClient` dung chung theo tung API worker, dong
client trong FastAPI lifespan khi application shutdown. Connection pool co
the dieu chinh bang:

```bash
KSERVE_MAX_CONNECTIONS=100
KSERVE_MAX_KEEPALIVE_CONNECTIONS=100
```

File lien quan:

- `src/fraud_detection/core/predict.py`
- `src/fraud_detection/core/api.py`

### 3. Dung Redis connection pool va pipeline

API su dung `BlockingConnectionPool` de gioi han va tai su dung ket noi Redis:

```bash
REDIS_POOL_MAX_CONNECTIONS=64
```

Hai lenh doc lich su giao dich va feature hash truoc day chay noi tiep:

```text
ZREVRANGE
HGETALL
```

Da dua vao mot Redis pipeline de giam so round-trip mang.

File lien quan:

- `src/fraud_detection/core/api.py`
- `src/fraud_detection/features/feature_store.py`

### 4. Khong chan asyncio event loop bang pandas

`build_model_inputs()` dung pandas va xu ly lai lich su giao dich. Day la tac
vu CPU-bound, dong bo. Chay truc tiep trong coroutine lam event loop khong the
phuc vu Redis va KServe I/O kip thoi.

Da dua feature building vao `ThreadPoolExecutor` rieng:

```bash
FEATURE_BUILD_WORKERS=1
```

Executor duoc gioi han de tranh tao qua nhieu thread pandas trong mot process.
Muon tang kha nang xu ly feature building nen scale API process hoac replica,
khong nen tang thread tuy y.

File lien quan:

- `src/fraud_detection/core/predict.py`

### 5. Chay Redis refresh va Postgres insert song song

Sau khi co prediction, API truoc day cho Redis refresh xong roi moi insert vao
Postgres. Hai tac vu I/O doc lap nay da duoc chay dong thoi:

```python
await asyncio.gather(
    refresh_features_for_user_card(...),
    save_transaction(...),
)
```

Postgres pool local duoc tang len:

```bash
POSTGRES_POOL_MAX_SIZE=20
```

File lien quan:

- `src/fraud_detection/core/predict.py`
- `infra/docker/docker-compose.yml`

### 6. Giam log trong benchmark

Benchmark mac dinh dung:

```bash
LOG_LEVEL=WARNING
```

Va Uvicorn chay voi `--no-access-log`. Viec nay giam I/O console va giup ket
qua load test on dinh hon.

Muon theo doi request log khi debug:

```bash
LOG_LEVEL=INFO docker compose -f infra/docker/docker-compose.yml \
  up -d --no-build --force-recreate api
docker logs -f docker-api-1
```

### 7. Chuyen performance test sang Locust

Script asyncio tu viet da duoc thay bang Locust scenario. Script Locust:

- Lay `user_id` va `card_id` hop le tu Postgres truoc khi chay.
- Tao payload `POST /score`.
- Kiem tra HTTP response va `transaction_id`.
- Dung `constant_throughput(1)` cho moi Locust user.

File lien quan:

- `tests/test_performance_api.py`
- `pyproject.toml`
- `uv.lock`

## Ket qua sau toi uu

Lan Locust gan nhat duoc chay voi cau hinh mac dinh:

```bash
API_WORKERS=5
FEATURE_BUILD_WORKERS=1
LOG_LEVEL=WARNING
```

Profile benchmark:

```bash
uv run locust -f tests/test_performance_api.py \
  --host http://localhost:1311 \
  --headless \
  --users 50 \
  --spawn-rate 50 \
  --run-time 10s \
  --stop-timeout 30s \
  --only-summary \
  --html locust-report.html \
  --csv locust-report
```

Ket qua console:

| Chi so | Ban dau | Sau toi uu |
|---|---:|---:|
| Request hoan tat | 500 | 369 |
| Loi | 5 | 0 |
| Throughput | 15.71 RPS | 35.08 RPS |
| Latency trung binh | 15.67 s | 1.15 s |
| p50 | 16.15 s | 1.10 s |
| p95 | 22.95 s | 2.40 s |
| p99 | 23.22 s | 3.30 s |
| Max | 23.31 s | 3.74 s |

Locust duoc cau hinh gan `50 RPS`, nhung khi response cham hon task interval
thi 50 users khong the luon duy tri du 50 RPS. Throughput tren la toc do thuc
te API hoan tat trong lan benchmark.

Output benchmark nam tai root project:

- `locust-report.html`
- `locust-report_stats.csv`
- `locust-report_stats_history.csv`
- `locust-report_failures.csv`

## Bottleneck con lai

Sau khi toi uu, diagnostic log cho thay:

| Cong doan | Latency thuong gap |
|---|---:|
| Redis fetch | 2-3 ms |
| pandas `build_model_inputs()` | 41-61 ms khi tai thap |
| KServe HTTP inference | 17-32 ms khi tai thap |
| Redis refresh va Postgres save song song | 2-9 ms |

Khi tai cao, pandas feature building bi queue CPU. Day la bottleneck chinh con
lai. Ham can toi uu tiep nam trong:

```text
src/fraud_detection/core/utils.py: build_model_inputs()
```

### Huong cai thien tiep theo

1. Tinh san rolling aggregate trong Redis thay vi dung pandas de xu ly lai
   lich su giao dich tren moi request.
2. Scale API theo process hoac replica neu can throughput cao hon.
3. Tach persistence khoi response path neu business requirement cho phep ghi
   du lieu bat dong bo.

## Luu y ve KServe LightGBM server

Compose hien truyen `--workers` va `--nthread` cho `lgbserver`. Tuy nhien image
`kserve/lgbserver` hien tai ep HTTP model server ve mot worker vi LightGBM
khong ho tro multiprocess trong implementation nay. `--workers` khong lam tang
so HTTP worker nhu ky vong.

Neu KServe tro thanh bottleneck, can scale bang nhieu container KServe phia sau
load balancer hoac dung serving implementation khac. Trong benchmark hien tai,
Redis va KServe chua phai bottleneck chinh.

