"""CHẨN ĐOÁN — chứng minh skew `merchant_id` bằng số, và làm nó hiện rõ trên Spark UI.

Job này KHÔNG ghi gì cả, chỉ đọc. Mục đích là tạo ra một application trong Spark
History Server mà mỗi phép đo là một job có TÊN RÕ RÀNG, thay vì đống
``count at NativeMethodAccessorImpl.java:0`` không biết stage nào là stage nào.

Đo hai thứ tách bạch:

  (A) SKEW TRONG DỮ LIỆU — phân bố số dòng theo từng key. Chỉ là groupBy count,
      không nói gì về Spark, nhưng cho biết key nào ĐÁNG nghi: max/median.

  (B) SKEW TRONG SHUFFLE — chạy lại đúng cửa sổ ``merch_30`` của
      dp3_training_features (``partitionBy(merchant_id) rangeBetween(-30d, 0)``),
      ĐO CẢ HAI cách tính distinct rồi kiểm chứng chúng ra cùng kết quả:
      ``size(collect_set)`` (bản cũ) và ``spark_windows.distinct_count_range``
      (bản tối ưu). Đây mới là thứ Spark thật sự phải gánh.

Vì sao (B) đắt hơn (A) rất nhiều dù cùng một key: ``collect_set`` trên
``rangeBetween`` KHÔNG có buffer trượt — mỗi dòng dựng lại set từ đầu cửa sổ, nên
chi phí là O(số dòng trong cửa sổ) cho MỖI dòng, tức bậc hai theo số dòng của một
merchant. Merchant nóng gấp đôi lưu lượng thì task đó tốn gấp bốn.

Chạy bằng spark-submit trong container Airflow::

    spark-submit ... skew_probe.py --source silver
    spark-submit ... skew_probe.py --source gold --skip-window

LƯU Ý về dữ liệu: ``curated/fact_transactions`` chỉ có những ngày DAG đã chạy
(DP2 ghi theo ``--date``, ``partitionOverwriteMode=dynamic``). Muốn ``--source
gold`` có đủ lịch sử như silver thì phải backfill trước::

    spark-submit ... dp2_silver_to_gold.py --stage fact --date all

Đọc kết quả trên UI (History Server, cổng 18080):

  Stages -> mở stage của job "B1."/"B2." -> Summary Metrics:
    * Duration / CPU  max/min > 5x    -> một task ôm phần việc lớn bất thường
    * Spill (memory)  khác 0          -> buffer cửa sổ không vừa RAM

  ĐỪNG đọc skew của cửa sổ này qua Shuffle Read: chi phí của ``collect_set`` trên
  ``rangeBetween`` tỉ lệ với BÌNH PHƯƠNG số dòng mỗi key, không tỉ lệ với byte.
  Đo thật trên 100,752 dòng: Shuffle Read lệch 1,01x (nhìn như không skew) trong
  khi Duration lệch 21x. Byte-per-task luôn trông cân bằng ở đây.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# Độ dài cửa sổ khai ở MỘT chỗ (xem shared/feature_windows.py).
sys.path.insert(0, os.environ.get("SHARED_DIR", "/opt/spark/shared"))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
import feature_windows as W  # noqa: E402
import spark_windows as SW  # noqa: E402

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


DISTINCT_COL = "merchant_distinct_cards_30d"


def _merch_30() -> Window:
    """Đúng cửa sổ ``merch_30`` của dp3_training_features."""
    return (Window.partitionBy("merchant_id").orderBy("ts")
            .rangeBetween(-W.MERCHANT_WINDOW_S, 0))


def _drain(spark: SparkSession, desc: str, df) -> float:
    """Ép thực thi df qua sink ``noop`` và trả về số giây.

    BẪY: KHÔNG dùng ``df.count()`` để ép thực thi. ``count()`` không cần giá trị
    của các cột window, nên optimizer cắt luôn node Window ra khỏi plan — job vẫn
    chạy, vẫn mất mười mấy giây quét parquet, nhưng shuffle read = 0 và tab SQL
    không có node ``Window`` nào. Đo được đúng cái không có gì.

    Sink ``noop`` thì phải sinh ra MỌI cột của mọi dòng nên không cắt được gì, mà
    vẫn không ghi byte nào ra đĩa — cách chuẩn để benchmark một phép biến đổi.
    """
    spark.sparkContext.setJobDescription(desc)
    t0 = time.perf_counter()
    df.write.format("noop").mode("overwrite").save()
    dt = time.perf_counter() - t0
    print(f"    {desc}: {dt:,.1f}s")
    return dt


def window_probe(spark: SparkSession, tx, verify: bool = True) -> None:
    """(B) merch_30: đo bản cũ và bản tối ưu trên CÙNG dữ liệu, rồi đối chiếu.

    Ba phép đo tách bạch để biết phần nào tối ưu được, phần nào không:

      B0  chỉ ``count`` trên merch_30           -> sàn, không đụng tới distinct
      B1  count + ``size(collect_set)``          -> bản cũ (dp3 trước khi sửa)
      B2  count + ``distinct_count_range``       -> bản tối ưu

    B1 - B0 mới là chi phí THẬT của cách tính distinct cũ; so thẳng B1 với B2 sẽ
    tính cả phần count vào cả hai bên và làm tỉ lệ tăng tốc trông nhỏ đi.
    """
    win = _merch_30()
    print(f"\n[B] merch_30 ({W.MERCHANT_WINDOW_S}s) — sink noop, không ghi gì")

    base = tx.withColumn("merchant_tx_count_30d", F.count(F.lit(1)).over(win))
    t0 = _drain(spark, "B0. merch_30 chỉ count (sàn, không distinct)", base)

    old = base.withColumn(DISTINCT_COL, F.size(F.collect_set("card_id").over(win)))
    t1 = _drain(spark, "B1. + distinct bằng size(collect_set) — BẢN CŨ", old)

    # Dựng từ tx chứ không từ base: distinct_count_range đọc lại
    # (merchant_id, card_id, ts) để dựng timeline, dựng từ base thì nhánh đó kéo
    # theo cả node Window của count và phép đo hết sạch ý nghĩa.
    new = (SW.distinct_count_range(tx, ["merchant_id"], "card_id",
                                   W.MERCHANT_WINDOW_S, DISTINCT_COL)
             .withColumn("merchant_tx_count_30d", F.count(F.lit(1)).over(win)))
    t2 = _drain(spark, "B2. + distinct bằng sự kiện — BẢN TỐI ƯU", new)

    d_old, d_new = t1 - t0, t2 - t0
    print(f"\n    riêng phần distinct:  cũ {d_old:,.1f}s  ->  mới {d_new:,.1f}s", end="")
    print(f"   ({d_old / d_new:,.1f}x)" if d_new > 0 else "")

    if not verify:
        print("    (bỏ qua kiểm chứng — hai bản CHƯA được đối chiếu)")
        return

    # Nhanh hơn mà sai thì vô nghĩa: đối chiếu từng dòng, không lấy mẫu.
    # Bước này chạy LẠI bản cũ nên tốn thêm đúng một lần t1.
    spark.sparkContext.setJobDescription("B3. kiểm chứng bản cũ == bản tối ưu")
    a = tx.select("id", F.size(F.collect_set("card_id").over(win)).alias("_v_old"))
    b = SW.distinct_count_range(tx, ["merchant_id"], "card_id",
                                W.MERCHANT_WINDOW_S, "_v_new").select("id", "_v_new")
    diff = a.join(b, "id").where(F.col("_v_old") != F.col("_v_new"))
    n_bad = diff.count()
    if n_bad:
        print(f"\n[B3] ✗ LỆCH {n_bad:,} dòng — bản tối ưu KHÔNG tương đương, đừng dùng:")
        diff.show(5, truncate=False)
        raise SystemExit(1)
    print("\n[B3] ✓ hai bản khớp trên toàn bộ số dòng (không lấy mẫu).")


def main() -> None:
    p = argparse.ArgumentParser(description="Chẩn đoán skew key shuffle")
    p.add_argument("--source", choices=["silver", "gold"], default="silver",
                   help="silver = staging/transactions (có ngay sau DP2 bước 1), "
                        "gold = curated/fact_transactions")
    p.add_argument("--skip-window", action="store_true",
                   help="Chỉ đo phân bố (A), bỏ qua phần window (B).")
    p.add_argument("--skip-verify", action="store_true",
                   help="Bỏ bước đối chiếu cũ==mới ở (B). Nhanh hơn đúng một lần "
                        "chạy bản cũ, đổi lại không còn bằng chứng tương đương.")
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
        window_probe(spark, tx, verify=not args.skip_verify)

    spark.stop()


if __name__ == "__main__":
    main()
