# Fraud Detection — Data Platform Flow

4 luồng, mỗi luồng đánh số và tô màu riêng:

| Màu | Luồng | Ý nghĩa |
|---|---|---|
| 🔵 xanh dương | **A1–A13** | Batch: generator → medallion → offline feature store → online |
| 🟠 cam | **B1–B6** | Streaming: producer → Kafka → Flink → online store |
| 🟢 xanh lá | **C1–C6** | Serving: request `/score` → feature → model → quyết định |
| 🟣 tím | **D1–D3** | Training: point-in-time join → model registry |
| ⚫ xám đứt | — | Control: Airflow điều phối (không truyền dữ liệu) |

## 1. Toàn cảnh hệ thống

```mermaid
flowchart TB
  %% ---------------- GENERATORS ----------------
  subgraph GEN["🐍 Data Generator (Python)"]
    G1["generate_offline.py<br/>300k tx / 365 ngày<br/>4 lỗi offline"]
    G2["generate_daily.py<br/>~800 tx / ngày"]
    G3["generate_stream.py<br/>20→200 msg/s<br/>3 lỗi streaming"]
  end

  %% ---------------- LAKEHOUSE ----------------
  subgraph MINIO["🪣 MinIO — Lakehouse (S3)"]
    SRC[("bucket: source<br/>parquet BẨN<br/>= data department")]
    BRZ[("bucket: raw = BRONZE<br/>transactions/event_date=…<br/>+ 5 snapshot dim/labels")]
    SLV[("bucket: staging = SILVER<br/>đã dedup + merge schema")]
    GLDF[("curated: fact_transactions<br/>partition event_date")]
    GLDD[("curated: dim_user / dim_card<br/>dim_merchant / dim_device<br/>SCD2: valid_from/to, is_current")]
  end

  %% ---------------- ENGINES ----------------
  subgraph SPARK["⚡ Spark (standalone cluster)"]
    SPK1["dp2_bronze_to_silver.py<br/>dropDuplicates(id) + mergeSchema"]
    SPK2["dp2_silver_to_gold.py<br/>fact + SCD2 merge"]
    SPK3["dp3_gold_to_features.py<br/>agg 90d/30d + graph"]
  end

  subgraph STREAM["🌊 Streaming"]
    T1[["Redpanda topic<br/>transactions"]]
    FLK["Flink SQL — velocity_5min<br/>watermark 90s → Deduplicate<br/>→ HOP window 5min/1min"]
    T2[["Redpanda topic<br/>card_velocity_5min<br/>(upsert)"]]
    BR["velocity_bridge.py<br/>store.push()"]
  end

  %% ---------------- FEATURE STORE ----------------
  subgraph FEAST["🍽️ Feast Feature Store"]
    PG[("Postgres warehouse<br/>OFFLINE store — schema application<br/>feat_card / feat_user / feat_merchant<br/>feat_device / feat_card_device<br/>feat_card_velocity")]
    RDS[("Redis<br/>ONLINE store<br/>116k key")]
  end

  %% ---------------- ORCHESTRATION ----------------
  AF{{"🌬️ Airflow<br/>dp1_ingest_bronze<br/>dp2_transform<br/>dp3_features"}}

  %% ---------------- CONSUMERS ----------------
  subgraph SERVE["🎯 Serving & Training"]
    REQ(["Giao dịch mới<br/>từ payment gateway"])
    API["Scoring API /score<br/>+ OnDemandFeatureView"]
    MODEL["Model<br/>LightGBM / XGBoost"]
    DEC["Decision engine<br/>allow / review / block"]
    TRN["Training pipeline"]
    REG[("Model Registry")]
  end

  %% ============ A · BATCH ============
  G1 -->|"A1 · parquet bẩn: skew 80% US,<br/>dup 1%, schema evo, high-card"| SRC
  G2 -->|"A2 · 1 partition / ngày"| SRC
  SRC -->|"A3 · DP1 copy_partition(event_date=ds)"| BRZ
  BRZ -->|"A4 · read transactions"| SPK1
  SPK1 -->|"A5 · dedup + mergeSchema"| SLV
  SLV -->|"A6 · read"| SPK2
  SPK2 -->|"A7 · fact_transactions"| GLDF
  BRZ -->|"A8 · read dim snapshot"| SPK2
  SPK2 -->|"A9 · SCD2 merge (đóng bản cũ,<br/>chèn version mới)"| GLDD
  GLDF -->|"A10 · window 90d / 30d"| SPK3
  GLDD -->|"A11 · dim current (is_current=true)"| SPK3
  SPK3 -->|"A12 · JDBC → 5 bảng feat_"| PG
  PG -->|"A13 · feast materialize-incremental<br/>latest-per-entity"| RDS

  %% ============ B · STREAMING ============
  G3 -->|"B1 · JSON, event-time=created_at<br/>burst / late 5-60s / dup 1.5%"| T1
  T1 -->|"B2 · consume"| FLK
  FLK -->|"B3 · card_tx_count/sum/avg_5min"| T2
  T2 -->|"B4 · consume"| BR
  BR -->|"B5 · push → online"| RDS
  BR -.->|"B6 · backfill cho training (TODO)"| PG

  %% ============ C · SERVING ============
  REQ -->|"C1 · POST /score {card_id, user_id,<br/>merchant_id, device_id, amount…}"| API
  API -->|"C2 · get_online_features(service)<br/>1 round-trip, ~1ms"| RDS
  RDS -->|"C3 · batch + streaming features"| API
  API -->|"C4 · + on-demand (log_amount, hour,<br/>geo_mismatch, card_is_new_device_90d)"| MODEL
  MODEL -->|"C5 · fraud score"| DEC
  DEC -->|"C6 · allow / review / block"| REQ

  %% ============ D · TRAINING ============
  PG -->|"D1 · get_historical_features<br/>point-in-time join"| TRN
  TRN -->|"D2 · model artifact"| REG
  REG -->|"D3 · load"| MODEL

  %% ============ CONTROL (Airflow) ============
  AF -.->|"schedule DP1 @daily"| BRZ
  AF -.->|"trigger DP2 (docker exec spark-submit)"| SPK1
  AF -.->|"trigger DP2 gold"| SPK2
  AF -.->|"trigger DP3"| SPK3
  AF -.->|"materialize"| RDS

  %% ---------------- STYLE ----------------
  classDef store fill:#e0f2fe,stroke:#0369a1,color:#0c4a6e
  classDef engine fill:#fef3c7,stroke:#b45309,color:#78350f
  classDef gen fill:#f3e8ff,stroke:#7e22ce,color:#4c1d95
  classDef serve fill:#dcfce7,stroke:#15803d,color:#14532d
  class SRC,BRZ,SLV,GLDF,GLDD,PG,RDS,T1,T2,REG store
  class SPK1,SPK2,SPK3,FLK,BR engine
  class G1,G2,G3 gen
  class REQ,API,MODEL,DEC,TRN serve

  %% màu theo luồng: A=xanh dương, B=cam, C=xanh lá, D=tím, control=xám
  linkStyle 0,1,2,3,4,5,6,7,8,9,10,11,12 stroke:#2563eb,stroke-width:2px
  linkStyle 13,14,15,16,17,18 stroke:#ea580c,stroke-width:2px
  linkStyle 19,20,21,22,23,24 stroke:#16a34a,stroke-width:2px
  linkStyle 25,26,27 stroke:#9333ea,stroke-width:2px
  linkStyle 28,29,30,31,32 stroke:#94a3b8,stroke-width:1px
```

## 2. Chi tiết: một request `/score` đi như thế nào

```mermaid
sequenceDiagram
  autonumber
  participant PG as Payment Gateway
  participant API as Scoring API
  participant F as Feast SDK
  participant R as Redis (online store)
  participant ODFV as OnDemandFeatureView
  participant M as Model
  participant D as Decision engine

  PG->>API: POST /score<br/>{card_id, user_id, merchant_id, device_id,<br/> amount_usd, billing_country, ip_country,<br/> email_purchaser, email_recipient, ts}
  Note over API: KHÔNG query Postgres —<br/>mọi thứ precompute đã ở Redis

  API->>F: get_online_features(fraud_detection_service, entity_rows)
  F->>R: HMGET × 5 entity key (1 pipeline round-trip)
  Note over R: card:8f6f842f → card_features + card_velocity_features<br/>user:… → user_features<br/>merchant:… → merchant_features<br/>device:… → device_features<br/>(card,device) → card_device_features

  R-->>F: BATCH: card_brand, card_type, is_virtual,<br/>card_tx_count/sum/avg_90d, customer_segment,<br/>kyc_level, user_country, user_device_count_30d,<br/>merchant_category/risk, device_distinct_users_30d,<br/>cd_tx_count_90d
  R-->>F: STREAMING: card_tx_count_5min = 37,<br/>card_amount_sum_5min = 125.82,<br/>card_amount_avg_5min = 3.40

  F->>ODFV: chạy transform trên (request + feature vừa đọc)
  ODFV-->>F: log_amount, hour, weekday, is_night,<br/>geo_mismatch=1, foreign_ip=1, recipient_differs=1,<br/>account_age_days, card_age_days,<br/>card_is_new_device_90d = (cd_tx_count_90d IS NULL)
  F-->>API: 1 feature vector (~30 cột) — ~1-3ms

  API->>M: predict(feature_vector)
  M-->>API: fraud_score = 0.94
  API->>D: score + rule
  D-->>API: BLOCK (velocity 37 tx/5min, avg $3.40,<br/>device mới, IP lệch → card testing)
  API-->>PG: {decision: "block", score: 0.94, reasons: [...]}
```

## 3. Ba tầng feature — ai tính, tính ở đâu

```mermaid
flowchart LR
  subgraph B["🔵 BATCH — Spark DP3, chạy @daily"]
    B1["card_tx_count_90d<br/>card_amount_sum/avg_90d"]
    B2["user_device_count_30d<br/>device_distinct_users_30d ← bắt fraud ring"]
    B3["dim attrs: card_brand, card_type, is_virtual,<br/>customer_segment, kyc_level, email_verified,<br/>user_country, merchant_category, merchant_risk_level"]
    B4["cd_tx_count_90d (cặp card-device)"]
  end
  subgraph S["🟠 STREAMING — Flink, liên tục"]
    S1["card_tx_count_5min<br/>card_amount_sum_5min<br/>card_amount_avg_5min"]
  end
  subgraph O["🟢 ON-DEMAND — tại request, không lưu store"]
    O1["log_amount, hour, weekday, is_night"]
    O2["geo_mismatch, foreign_ip, recipient_differs"]
    O3["account_age_days, card_age_days<br/>(= ts − *_created_at)"]
    O4["card_is_new_device_90d<br/>(= cd_tx_count_90d IS NULL)"]
  end
  B --> PGS[("Postgres offline")] --> RD[("Redis online")]
  S --> RD
  RD --> V["feature vector"]
  O --> V
  V --> MDL["Model"]

  classDef s fill:#e0f2fe,stroke:#0369a1
  class PGS,RD s
```

## Ghi chú kỹ thuật

- **Airflow → Spark**: `docker exec spark-master spark-submit` (mount `/var/run/docker.sock` + `group_add: 984`). Phương án prod nên đổi sang Spark Connect / spark-on-k8s.
- **Watermark 90s**: đánh đổi *freshness ↔ completeness*. Watermark lớn bắt được nhiều hàng muộn nhưng feature ra chậm → vô dụng cho fraud real-time. Producer trễ tối đa 60s nên 90s là đủ.
- **`feat_card_velocity` (B6) hiện rỗng**: velocity mới chỉ đi vào online store. Muốn train với velocity phải backfill lịch sử vào bảng này.
- **Point-in-time (D1)**: SCD2 `valid_from_ts/valid_to_ts` + `event_timestamp` của bảng `feat_` cho phép lấy đúng giá trị *tại thời điểm* giao dịch → chống data leakage.
