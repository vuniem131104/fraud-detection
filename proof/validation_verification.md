# Validation & Verification

## 1. Unit Test với Test Coverage > 90%
Trong dự án này, hệ thống Web API đã được kiểm thử toàn diện bằng cách sử dụng `TestClient` của FastAPI kết hợp với các kỹ thuật `fixture` và `mock` (như `AsyncMock`, `MagicMock`) để cô lập các phụ thuộc bên ngoài (Postgres, Redis, mô hình KServe).
Mục đích là để kiểm tra xem business logic và các thành phần cốt lõi của API có hoạt động chính xác không mà không cần các database thật.
Kết quả cho thấy **Test Coverage đạt 98.48%** (vượt xa mục tiêu 90%).

*(Screenshot chụp lúc chạy lệnh `pytest --cov`)*
```text
================================ tests coverage ================================
_______________ coverage: platform linux, python 3.13.9-final-0 ________________

Name                                            Stmts   Miss Branch BrPart   Cover   Missing
--------------------------------------------------------------------------------------------
src/fraud_detection/core/api.py                   114      0      2      0 100.00%
src/fraud_detection/core/models.py                 35      0      0      0 100.00%
src/fraud_detection/core/predict.py               141      0     16      0 100.00%
src/fraud_detection/core/utils.py                 304      3    118      9  97.16%
src/fraud_detection/features/feature_store.py      48      0      8      0 100.00%
...
--------------------------------------------------------------------------------------------
TOTAL                                             646      3    144      9  98.48%
Required test coverage of 90.0% reached. Total coverage: 98.48%
210 passed in 7.59s
```

## 2. Kỹ thuật Equivalence Partitioning (EP) & Boundary Value Analysis (BVA)
Chúng ta đã áp dụng kỹ thuật EP và BVA để thiết kế test cases nhằm tối đa hóa khả năng tìm lỗi với số lượng test case tối thiểu. File `tests/test_ep_bva.py` sử dụng `pytest.mark.parametrize` để truyền các giá trị đại diện cho từng phân vùng tương đương và các giá trị biên.

Ví dụ: Validation kiểm tra số tiền `amount_usd > 0`, chúng ta test tại ngay biên `0.0`, ngay trên biên `0.001`, giá trị âm `-10.0`, và giá trị hợp lệ cực lớn `1_000_000.0`.
*(Screenshot chạy pytest cho file `test_ep_bva.py`)*
```text
$ pytest tests/test_ep_bva.py -q
........................................................................ [ 75%]
.......................                                                  [100%]
95 passed in 0.54s
```

## 3. Mutation Testing
Để đánh giá hiệu quả thực sự của bộ test (kiểm tra xem test suite có thực sự bắt được lỗi khi code bị thay đổi sai logic hay không), chúng ta đã dùng `mutmut`. 
**Lưu ý quan trọng**: Để tối ưu hiệu suất, cấu hình `mutmut` (`mutmut.toml`) được thiết lập chỉ mutate (đột biến) những file bị thay đổi (code changed) gần đây, cụ thể là `api.py` và `utils.py`, không mutate toàn bộ codebase.
Mutation score kỳ vọng đạt chỉ tiêu > 80% (tức là bộ test đã "giết" được hầu hết các đột biến, chứng tỏ các câu lệnh assert rất chặt chẽ).

*(Bạn có thể capture screenshot màn hình lệnh `mutmut run` và kết quả `mutmut results` ở đây sau khi nó chạy xong)*
```text
⠇ Generating mutants
    done in 5360ms (2 files mutated, 0 ignored, 0 unmodified)
⠙ Running stats...
```

## 4. Idempotency & Property-Based Testing
Để đảm bảo mô hình và các hàm xử lý hoạt động nhất quán (consistent), chúng ta sử dụng thư viện `hypothesis`. Khác với example-based testing (sử dụng các input cố định), property-based testing tự động sinh ra hàng trăm input ngẫu nhiên nhằm phá vỡ hệ thống và tìm ra các edge-case bugs (bug ở các trường hợp cực đoan). 

Các đặc điểm (properties) được kiểm chứng gồm: 
- Hàm `normalize_email` là idempotent (chạy nhiều lần qua hàm kết quả không đổi).
- Thuật toán `hash_password` là deterministic (cùng một mật khẩu luôn ra chung 1 mã hash).
- Tính nhất quán của `build_model_inputs` schema luôn giống hệt sau nhiều lần gọi.

*(Screenshot chạy pytest `test_property_based.py`)*
```text
$ pytest tests/test_property_based.py -q
...............                                                          [100%]
15 passed in 2.79s
```

## 5. Load Test Web API
Chúng ta sử dụng công cụ `locust` để tiến hành load test Web API (giả lập lượng traffic truy cập vào endpoint `/score` và `/health`), mục tiêu là đo lường SLA về throughput (requests/second - req/s) và latency (độ trễ). 
Báo cáo HTML được sinh ra sau quá trình load test. Kết quả cho thấy API xử lý rất tốt và ổn định.

**Đạt được các mốc SLA:**
- Throughput 17.7 req/s (vượt mốc mục tiêu 10 req/s)
- p50 latency 1 ms (nhỏ hơn rất nhiều so với mục tiêu 200 ms)
- p95 latency 2 ms (nhỏ hơn mục tiêu 500 ms)
- Error rate 0.00% (không có request nào lỗi)

*(Screenshot Locust HTML report)*
![Locust Load Test HTML Report](./locust_report.png)
