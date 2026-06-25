# Unit tests — Fraud Detection

Unit test suite cho Web API và core logic, dùng **pytest**, **fixtures** và **mocks**.
Đạt **test coverage > 90%** (hiện tại **98.23%**) trên package `src/fraud_detection`.

## Cách chạy / How to run

```bash
# Chạy toàn bộ test + báo cáo coverage ra terminal
uv run pytest --cov --cov-report=term-missing

# Chạy + sinh báo cáo HTML (mở htmlcov/index.html bằng trình duyệt)
uv run pytest --cov --cov-report=html

# Chỉ chạy test cho Web API (verbose)
uv run pytest tests/test_api.py -v
```

Cấu hình nằm trong `pyproject.toml` (`[tool.pytest.ini_options]`, `[tool.coverage.*]`),
trong đó `fail_under = 90` để CI tự fail nếu coverage tụt dưới 90%.

## Cấu trúc / Structure

| File | Bao phủ / Covers |
| --- | --- |
| `conftest.py` | **Fixtures** dùng chung: schema giả lập, payload giao dịch, redis state, và các **mock** cho service / Postgres / Redis; `client` (FastAPI `TestClient`). |
| `test_api.py` | **Web API**: `/health`, `/ready`, `/users`, `/score`, lifespan, helpers. Dùng `TestClient` + dependency override + mock. |
| `test_predict.py` | `FraudDetectionService`: gọi KServe (fake bằng `httpx.MockTransport`), pipeline `predict`, open/close. |
| `test_core_utils.py` | Feature engineering: normalize, build base/aggregate features, `build_model_inputs`. |
| `test_features.py` | Redis feature store (decode + `get_txs`) và key builders. |
| `test_models.py` | Pydantic validation cho input/output. |

## Fixtures & Mocks (yêu cầu coursework)

- **Fixtures**: định nghĩa trong `conftest.py` và được pytest tự inject vào test theo tên tham số
  (vd: `schema`, `transaction_payload`, `client`, `mock_service`, `mock_database`, `mock_redis`).
- **Mocks**: `unittest.mock.AsyncMock` / `MagicMock` thay cho Postgres, Redis, Kafka và service;
  `httpx.MockTransport` giả lập ML inference engine (KServe). Nhờ vậy test **không cần** backend thật.
- **Web API testing**: `fastapi.testclient.TestClient` gửi HTTP request thật vào app; service được tiêm
  qua `app.dependency_overrides`, còn Postgres/Redis gắn vào `app.state` — lifespan thật bị bỏ qua.

## Chụp ảnh minh chứng / Proof screenshots

1. **Coverage > 90%** — chạy `uv run pytest --cov --cov-report=term-missing` và chụp bảng coverage cuối (dòng `TOTAL ... 98.23%`).
2. **Báo cáo HTML** — chạy với `--cov-report=html`, mở `htmlcov/index.html` trong trình duyệt và chụp.
3. **Test Web API** — chạy `uv run pytest tests/test_api.py -v` và chụp danh sách test PASSED.
