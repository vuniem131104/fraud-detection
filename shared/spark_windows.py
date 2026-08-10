"""Đếm distinct trên cửa sổ event-time bằng SỰ KIỆN, thay cho ``size(collect_set)``.

Vì sao cần module này
---------------------
``size(collect_set(x).over(rangeBetween(-W, 0)))`` là cách duy nhất viết được
"đếm distinct trên cửa sổ trượt" bằng window function của Spark. Nhưng frame
trượt KHÔNG có buffer tăng dần: mỗi dòng dựng lại tập hợp từ đầu cửa sổ, nên chi
phí là O(số dòng trong cửa sổ) cho MỖI dòng — bậc hai theo số dòng của một khoá.

Đo trên ``dp3_training_features`` (100,752 giao dịch / 124 ngày, local[2]):

    stage Window(merchant_id)   task 0:  6.4 s / 47,129 dòng
                                task 1: 71.7 s / 53,623 dòng
    -> 78 s trong tổng 166 s executor time của cả job

Hai task nhận gần bằng nhau về DỮ LIỆU (lệch 14%) mà lệch 11 lần về THỜI GIAN:
một merchant ôm 24,5% số dòng, và mọi dòng cùng ``merchant_id`` **bắt buộc** nằm
trên một task. Không config nào chữa được — ``adaptive.skewJoin`` chỉ tách
partition lệch của JOIN, còn partition của window thì theo định nghĩa không tách
được. Cách duy nhất là đổi thuật toán.

Thuật toán
----------
Thay vì hỏi "tại thời điểm t có bao nhiêu giá trị khác nhau trong ``[t-W, t]``",
hỏi ngược lại: MỘT LẦN XUẤT HIỆN đóng góp 1 vào những t nào.

Lần xuất hiện tại ``s`` của giá trị ``v`` (lần kế tiếp của cùng ``v`` là ``nxt``)
được tính đúng với những ``t`` thoả::

    s <= t <= s + W     nằm trong cửa sổ kết thúc tại t
    t <  nxt            là lần xuất hiện GẦN NHẤT của v tính tới t

Điều kiện thứ hai làm mỗi giá trị chỉ được đếm một lần dù xuất hiện nhiều lần
trong cửa sổ. Vậy đóng góp của nó là một KHOẢNG liên tục trên trục t, và câu hỏi
ban đầu thành "bao nhiêu khoảng phủ điểm t" — giải bằng chuỗi cộng dồn::

    +1 tại s
    -1 tại nxt      nếu nxt <= s + W    khoảng đóng TẠI nxt (t = nxt là hết)
    -1 SAU s + W    nếu không           khoảng đóng SAU s+W (t = s+W VẪN tính)

Hai loại kết thúc khác nhau ở chỗ có tính mốc bằng hay không, nên phải cộng dồn
riêng: ``di`` (tính cả mốc) và ``ds`` (chỉ có hiệu lực SAU mốc). Giá trị tại t::

    cum(di)[et <= t] + cum(ds)[et <= t] - ds[t]

Trừ ``ds[t]`` chính là cách khử "bằng mốc" mà không cần epsilon — và đây là chi
tiết bắt buộc phải đúng, vì ``rangeBetween(-W, 0)`` đóng ở CẢ HAI đầu: dòng cách
đúng W giây vẫn nằm trong cửa sổ. Cộng epsilon vào mốc thì sai đúng ở biên đó.

Chi phí và giới hạn
-------------------
4 lần shuffle, O(n log n). Skew **không biến mất**: chuỗi cộng dồn vẫn
``partitionBy(entity)`` nên merchant nóng vẫn nằm trọn một task. Nhưng frame
``unboundedPreceding -> currentRow`` là frame TĂNG DẦN — Spark chỉ cộng thêm dòng
mới, không dựng lại buffer — nên phần việc của task đó là **tuyến tính** theo số
dòng chứ không còn bậc hai. Lệch 11x về thời gian thành lệch cỡ tỉ lệ số dòng.

Chỉ dùng ở nơi CÓ đo được là tốn. 9 feature distinct còn lại của dp3 chạy trên
khoá đều (card/user/device, max 234 dòng/khoá) nên cửa sổ nhỏ, bậc hai không
đáng kể — đổi sang đây chỉ thêm shuffle mà không nhanh hơn.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def distinct_count_range(
    df: DataFrame,
    part: list[str],
    value_col: str,
    seconds: int,
    out_col: str,
    ts_col: str = "ts",
) -> DataFrame:
    """Thêm cột ``out_col`` = số giá trị ``value_col`` khác nhau trong ``[ts-seconds, ts]``.

    Tương đương chính xác với::

        F.size(F.collect_set(value_col).over(
            Window.partitionBy(*part).orderBy(ts_col).rangeBetween(-seconds, 0)))

    kể cả ở hai chỗ dễ lệch: ``value_col`` NULL không được đếm (giống
    ``collect_set``), và dòng cách đúng ``seconds`` giây VẪN nằm trong cửa sổ.

    ``ts_col`` phải là epoch giây kiểu double — cùng cột dùng cho ``rangeBetween``.
    Nó vừa là mốc thời gian vừa là khoá join ngược lại, và phép join là so sánh
    double: an toàn vì giá trị phía ``df`` không bị tính toán gì, so bằng bit.
    """
    keys = list(part)
    p = f"_dcr_{out_col}"
    ev, et, di, ds, nxt = f"{p}_ev", f"{p}_et", f"{p}_di", f"{p}_ds", f"{p}_nxt"

    has_val = F.col(value_col).isNotNull()

    # nxt = lần xuất hiện KẾ TIẾP của cùng (entity, value). Shuffle theo
    # (entity, value) nên merchant nóng được trải ra theo card_id, không dồn task.
    pair = Window.partitionBy(*keys, value_col).orderBy(ts_col)
    marked = (df.select(*keys, value_col, ts_col)
                .withColumn(nxt, F.lead(ts_col).over(pair)))

    # Mọi dòng đều phát sự kiện mở tại ts — kể cả dòng có value NULL (delta 0).
    # Nhờ vậy MỌI thời điểm truy vấn đều có mặt trong timeline, không thì dòng
    # value NULL sẽ join hụt và nhận null thay vì con số đúng.
    start = F.struct(
        F.col(ts_col).alias("et"),
        F.when(has_val, F.lit(1)).otherwise(F.lit(0)).alias("di"),
        F.lit(0).alias("ds"),
    )
    # nxt NULL -> phép so sánh ra NULL -> rơi vào otherwise: hết hiệu lực SAU
    # ts + seconds. Đúng nghĩa "không còn lần xuất hiện nào nữa".
    expire = F.when(
        has_val,
        F.when(
            F.col(nxt) <= F.col(ts_col) + F.lit(seconds),
            F.struct(F.col(nxt).alias("et"), F.lit(-1).alias("di"), F.lit(0).alias("ds")),
        ).otherwise(
            F.struct((F.col(ts_col) + F.lit(seconds)).alias("et"),
                     F.lit(0).alias("di"), F.lit(-1).alias("ds")),
        ),
    )

    events = (marked
              .select(*keys, F.explode(F.array(start, expire)).alias(ev))
              .where(F.col(ev).isNotNull())        # dòng value NULL không có sự kiện đóng
              .select(*keys,
                      F.col(ev).getField("et").alias(et),
                      F.col(ev).getField("di").alias(di),
                      F.col(ev).getField("ds").alias(ds)))

    # Gộp theo mốc TRƯỚC khi cộng dồn: nhiều giao dịch cùng một mốc thời gian phải
    # cùng đọc ra một giá trị, và gộp rồi thì mỗi (entity, et) chỉ còn một dòng
    # nên "cộng dồn tới currentRow" không phụ thuộc thứ tự trong nhóm.
    agg = events.groupBy(*keys, et).agg(F.sum(di).alias(di), F.sum(ds).alias(ds))

    cum = (Window.partitionBy(*keys).orderBy(et)
                 .rangeBetween(Window.unboundedPreceding, Window.currentRow))
    timeline = agg.select(
        *keys,
        F.col(et).alias(ts_col),
        (F.sum(di).over(cum) + F.sum(ds).over(cum) - F.col(ds))
        .cast("long").alias(out_col),
    )

    # Timeline có cả mốc hết hạn (ts + seconds) không phải giao dịch nào cả; join
    # theo (entity, ts) tự bỏ chúng đi. Mỗi (entity, et) là duy nhất nên join
    # không nhân dòng.
    return df.join(timeline, keys + [ts_col], "left")
