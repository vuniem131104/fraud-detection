"""CHẨN ĐOÁN — chứng minh skew `merchant_id` bằng số, và làm nó hiện rõ trên Spark UI.

Job này KHÔNG ghi gì cả, chỉ đọc. Mục đích là tạo ra một application trong Spark
History Server mà mỗi phép đo là một job có TÊN RÕ RÀNG, thay vì đống
``count at NativeMethodAccessorImpl.java:0`` không biết stage nào là stage nào.

Đo hai thứ tách bạch:

  (A) SKEW TRONG DỮ LIỆU — phân bố số dòng theo từng key. Chỉ là groupBy count,
      không nói gì về Spark, nhưng cho biết key nào ĐÁNG nghi: max/median.

  (B) SKEW TRONG SHUFFLE — chạy lại đúng cửa sổ ``merch_30`` của
      dp3_training_features (``partitionBy(merchant_id) rangeBetween(-30d, 0)``
      + ``size(collect_set(card_id))``). Đây mới là thứ Spark thật sự phải gánh,
      và là stage cần mở trên UI.

Vì sao (B) đắt hơn (A) rất nhiều dù cùng một key: ``collect_set`` trên
``rangeBetween`` KHÔNG có buffer trượt — mỗi dòng dựng lại set từ đầu cửa sổ, nên
chi phí là O(số dòng trong cửa sổ) cho MỖI dòng, tức bậc hai theo số dòng của một
merchant. Merchant nóng gấp đôi lưu lượng thì task đó tốn gấp bốn.

Chạy bằng spark-submit trong container Airflow::

    spark-submit ... skew_probe.py --source silver
    spark-submit ... skew_probe.py --source gold --skip-window

Đọc kết quả trên UI (History Server, cổng 18080):

  Stages -> mở stage của job "B. window merch_30" -> Summary Metrics:
    * Duration        max/median > 5x   -> một task ôm phần việc lớn bất thường
    * Shuffle Read    max/median lệch   -> lệch do DỮ LIỆU, không phải máy chậm
    * Spill (memory)  khác 0            -> buffer cửa sổ không vừa RAM
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# Độ dài cửa sổ khai ở MỘT chỗ (xem shared/feature_windows.py).
sys.path.insert(0, os.environ.get("SHARED_DIR", "/opt/spark/shared"))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
import feature_windows as W  # noqa: E402

LAKE_ROOT = os.environ["LAKE_ROOT"]
if not LAKE_ROOT.endswith("/"):
    LAKE_ROOT += "/"

SILVER = f"{LAKE_ROOT}staging/transactions"
FACT = f"{LAKE_ROOT}curated/fact_transactions"

# Các key mà job DP3 dùng làm khoá shuffle (partitionBy / groupBy).
KEYS = ["merchant_id", "card_id", "user_id", "device_id"]


def build_spark() -> SparkSession:
    """SparkSession đọc GCS.

    Cấu hình filesystem (gcs-connector + auth ADC) do spark-submit truyền vào
    bằng --conf, xem SPARK_CONF trong airflow/dags/ml_pipeline.py.

    KHÔNG set shuffle.partitions ở đây: để job chạy đúng cấu hình mặc định mà
    dp3_* đang chạy, nếu không thì con số đo được không nói về pipeline thật.
    """
    return SparkSession.builder.appName("skew_probe").getOrCreate()


def describe_key(spark: SparkSession, tx, key: str) -> dict:
    """(A) Phân bố số dòng theo ``key`` + top-5 key nặng nhất."""
    spark.sparkContext.setJobDescription(f"A. phân bố key: {key}")
    per_key = tx.groupBy(key).count().cache()
    agg = per_key.agg(
        F.count("*").alias("keys"),
        F.expr("percentile_approx(count, 0.5)").alias("median"),
        F.expr("percentile_approx(count, 0.99)").alias("p99"),
        F.max("count").alias("max"),
        F.sum("count").alias("rows"),
    ).first()
    top = per_key.orderBy(F.desc("count")).limit(5).collect()
    per_key.unpersist()

    median = agg["median"] or 0
    ratio = (agg["max"] / median) if median else float("nan")
    print(f"\n[A] {key}")
    print(f"    số key={agg['keys']:,}  median={median:,.0f}  "
          f"p99={agg['p99']:,.0f}  max={agg['max']:,}  max/median={ratio:,.0f}x")
    print(f"    key nặng nhất chiếm {agg['max'] / agg['rows']:.1%} tổng số dòng")
    for i, r in enumerate(top, 1):
        print(f"      {i}. {r[key]}  {r['count']:,} dòng")
    return {"key": key, "keys": agg["keys"], "median": median,
            "max": agg["max"], "ratio": ratio,
            "share": agg["max"] / agg["rows"]}


def window_probe(spark: SparkSession, tx) -> None:
    """(B) Chạy lại đúng cửa sổ merch_30 của dp3_training_features.

    BẪY: KHÔNG dùng ``out.count()`` để ép thực thi. ``count()`` không cần giá trị
    của hai cột window, nên optimizer cắt luôn node Window ra khỏi plan —
    job vẫn chạy, vẫn mất mười mấy giây quét parquet, nhưng shuffle read = 0 và
    tab SQL không có node ``Window`` nào. Đo được đúng cái không có gì.

    Sink ``noop`` thì phải sinh ra MỌI cột của mọi dòng nên không cắt được gì,
    mà vẫn không ghi byte nào ra đĩa — cách chuẩn để benchmark một phép biến đổi.
    """
    spark.sparkContext.setJobDescription(
        f"B. window merch_30 partitionBy(merchant_id) + collect_set "
        f"({W.MERCHANT_WINDOW_S}s)")
    win = (Window.partitionBy("merchant_id").orderBy("ts")
           .rangeBetween(-W.MERCHANT_WINDOW_S, 0))
    out = (tx.withColumn("merchant_tx_count_30d", F.count(F.lit(1)).over(win))
             .withColumn("merchant_distinct_cards_30d",
                         F.size(F.collect_set("card_id").over(win))))
    out.write.format("noop").mode("overwrite").save()
    print("\n[B] window merch_30 đã chạy THẬT (sink noop) — mở stage này trên UI.\n"
          "    Kiểm nhanh là đúng stage: tab SQL của query này phải có node "
          "`Window`,\n    và stage tương ứng phải có Shuffle Read > 0.")


def main() -> None:
    p = argparse.ArgumentParser(description="Chẩn đoán skew key shuffle")
    p.add_argument("--source", choices=["silver", "gold"], default="silver",
                   help="silver = staging/transactions (có ngay sau DP2 bước 1), "
                        "gold = curated/fact_transactions")
    p.add_argument("--skip-window", action="store_true",
                   help="Chỉ đo phân bố (A), bỏ qua phần window (B).")
    args = p.parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    path = SILVER if args.source == "silver" else FACT
    spark.sparkContext.setJobDescription(f"0. đọc {args.source}")
    tx = (spark.read.option("mergeSchema", "true").parquet(path)
               .withColumn("ts", F.col("created_at").cast("double")))
    total = tx.count()
    print(f"\n=== skew_probe trên {args.source} ({path}) — {total:,} dòng ===")

    results = [describe_key(spark, tx, k) for k in KEYS]

    print("\n=== TỔNG HỢP (A) ===")
    print(f"{'key':<14}{'số key':>10}{'median':>10}{'max':>10}"
          f"{'max/median':>12}{'% tổng':>9}")
    for r in results:
        print(f"{r['key']:<14}{r['keys']:>10,}{r['median']:>10,.0f}"
              f"{r['max']:>10,}{r['ratio']:>11,.0f}x{r['share']:>8.1%}")

    worst = max(results, key=lambda r: r["ratio"])
    print(f"\n-> key lệch nhất: {worst['key']} ({worst['ratio']:,.0f}x). "
          f"Chỉ đáng lo nếu job có partitionBy/groupBy theo key đó — "
          f"dp3_training_features có, với merchant_id.")

    if not args.skip_window:
        window_probe(spark, tx)

    spark.stop()


if __name__ == "__main__":
    main()
