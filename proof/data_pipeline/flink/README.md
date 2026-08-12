# Window processing trong Flink

Code: [`flink/sql/realtime_features.sql`](../../flink/sql/realtime_features.sql) — một job,
hai HOP window, hai sink `upsert-kafka`.

| Window | Size / slide | Khoá | Output |
|---|---|---|---|
| merchant | 10 phút / 1 phút | `merchant_id` | count · distinct cards · avg amount |
| device | 1 giờ / 5 phút | `device_id` | count · distinct users · distinct cards |

```sql
-- dòng 152-196
EXECUTE STATEMENT SET
BEGIN
INSERT INTO merchant_rt_sink
SELECT merchant_id, window_end,
       COUNT(*), COUNT(DISTINCT card_id), AVG(amount_usd)
FROM TABLE(
  HOP(TABLE tx_dedup, DESCRIPTOR(created_at),
      INTERVAL '1' MINUTE,      -- slide
      INTERVAL '10' MINUTE)     -- size = MERCHANT_RT_WINDOW_S
)
GROUP BY merchant_id, window_start, window_end;
...
END;
```

Cả hai `INSERT` nằm trong một `STATEMENT SET` → source đọc Kafka **một lần**, dedup một
lần cho cả hai nhánh. Tách hai file sẽ thành hai job, hai consumer group, đọc trùng topic.

## Ba dòng khiến window chạy được

Bỏ dòng nào trong ba dòng này thì job vẫn RUNNING mà output bằng 0:

| Dòng | Code | Vì sao |
|---|---|---|
| [88](../../flink/sql/realtime_features.sql#L88) | `WATERMARK FOR created_at AS created_at - INTERVAL '90' SECOND` | Window chỉ phát khi watermark vượt `window_end`. 90s = độ trễ tối đa producer tiêm vào |
| [57](../../flink/sql/realtime_features.sql#L57) | `table.exec.source.idle-timeout = 10s` | Watermark toàn cục = MIN mọi subtask. Nhịp ~817 tx/ngày → partition im lặng ghim watermark xuống đáy, window không bao giờ đóng |
| [143-150](../../flink/sql/realtime_features.sql#L143-L150) | `ROW_NUMBER() ... ORDER BY created_at ASC`, `WHERE rn = 1` | Dedup keep-**first** cho stream append-only. Đổi `DESC` → keep-last → changelog có retract → window aggregation từ chối input |

## Kiểm chứng đang chạy

```bash
docker compose exec flink-jobmanager /opt/flink/bin/flink list
```

Dashboard `localhost:8082` (qua SSH tunnel) → job `realtime_features` → DAG có hai nhánh
`WindowAggregate`, cột Records Sent > 0.

Window **không** phát ngay: watermark = `max event-time − 90s` và chỉ tiến khi có message
mới. Ở nhịp 1 message/~105 giây, một giao dịch xuất hiện ở topic kết quả sau **2–4 phút**.
Topic rỗng ngay sau khi submit là bình thường.
