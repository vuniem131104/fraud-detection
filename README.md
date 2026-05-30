# Fraud Detection System

End-to-end real-time fraud detection: streaming ingestion → feature engineering → ML scoring → decisioning → feedback loop.

## Architecture

```
Kafka (transactions)
   │
   ├──► Flink streaming jobs ──► Online feature store (Redis)
   │                          └─► Offline feature store (Iceberg/S3)
   │
   └──► Scoring API (FastAPI)
          │
          ├─► Rules engine
          ├─► ML models (XGBoost + GNN)
          └─► Decision engine ──► allow / step-up / review / block
                                       │
                                       └─► Case management + feedback
                                              │
                                              └─► Training pipeline ──► Model registry
```

## Layout

| Path | What's there |
|---|---|
| `src/fraud_detection/ingestion/` | Kafka producers/consumers, schema registry |
| `src/fraud_detection/streaming/` | Flink/PyFlink jobs for real-time features |
| `src/fraud_detection/features/` | Feature definitions (Feast), transformers |
| `src/fraud_detection/models/` | Training pipelines + inference wrappers |
| `src/fraud_detection/rules/` | Rule engine with hot-reload |
| `src/fraud_detection/scoring/` | Orchestrates rules + models into a risk score |
| `src/fraud_detection/decision/` | Maps score to action (allow/block/review) |
| `src/fraud_detection/feedback/` | Label ingestion (chargebacks, analyst reviews) |
| `src/fraud_detection/monitoring/` | Drift, latency, KPI metrics |
| `src/fraud_detection/api/` | FastAPI service exposing `/score` |
| `airflow/dags/` | Batch retraining, label backfill, monitoring DAGs |
| `infra/` | Docker, K8s, Terraform |

## Quickstart

```bash
make install        # poetry install
make up             # docker-compose: kafka + redis + postgres
make seed           # produce sample transactions
make serve          # run scoring API on :8000
make test           # pytest
```

## SLAs

- **Hot path** (score endpoint): p99 < 150ms
- **Feature freshness** (real-time aggs): < 5s
- **Model refresh**: weekly (challenger), daily (drift check)
