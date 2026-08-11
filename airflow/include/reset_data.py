"""Xoá sạch DATA của toàn hệ, GIỮ NGUYÊN mọi STRUCTURE — để nạp lại từ đầu.

Cái gì bị xoá / cái gì được giữ
-------------------------------
=============  ==========================================  =============================
Thành phần     XOÁ                                         GIỮ
=============  ==========================================  =============================
Flink          job đang chạy + checkpoint đã retain        session cluster, file SQL
Kafka          toàn bộ record trong 3 topic + offset group topic, partition, config
Postgres       mọi dòng trong ``ops.*``, ``application.*``  database, schema, bảng, index
Redis          toàn bộ key của online store (FLUSHDB)      instance
Spark          event log (lịch sử UI)                      volume
=============  ==========================================  =============================

Một lưu ý về cột GIỮ của Kafka: hai topic ``*_rt`` là ``cleanup.policy=compact``, mà
Kafka từ chối DeleteRecords trên topic không có ``delete`` trong policy. Script nới
policy tạm thời rồi TRẢ LẠI đúng giá trị đọc được lúc đầu, và đọc lại từ broker để
xác nhận — xem ``reset_kafka``. Kết thúc thành công thì config y như trước.

KHÔNG nằm trong phạm vi (cố ý):

* **Cloud Storage** — bạn tự xoá folder rồi tạo lại.
* **Feast registry** (``feast-registry``) — chứa ĐỊNH NGHĨA feature chứ không phải
  data; chủ nó là ``feast apply``. Xoá là mất entity/feature view và phải apply lại.
  Materialization interval trong registry cũng không cần reset: DAG dùng ``feast
  materialize`` với khoảng tường minh, không dùng ``materialize-incremental``
  (xem ``MATERIALIZE`` trong ``dags/ml_pipeline.py``).
* **Airflow metadata** (``airflow``) — lịch sử DAG run không phải data pipeline. Muốn
  xoá thì ``airflow db clean``, đó là việc khác.

Thứ tự các bước KHÔNG tuỳ ý: Flink trước (nó là writer duy nhất chạy 24/7 mà
compose không stop được bằng ``docker compose stop``), rồi Kafka, rồi các store hạ
nguồn. Xoá Postgres/Redis trước khi cắt nguồn ghi thì dữ liệu mới lại chảy vào giữa
lúc đang xoá.

Chạy — PHẢI stop 3 service streaming trước, nếu không chúng ghi lại ngay::

    docker compose stop stream-generator ops-ingest feature-bridge

    # 1. xem trước (mặc định là dry-run, KHÔNG xoá gì)
    docker compose exec airflow-scheduler python -m include.reset_data

    # 2. xoá thật
    docker compose exec airflow-scheduler python -m include.reset_data --apply

    # chỉ một phần
    ... --only kafka,redis --apply

Muốn xoá luôn checkpoint của Flink thì volume phải được mount vào container chạy
script — service ``airflow-*`` không mount nó, nên dùng ``run`` thay cho ``exec``::

    docker compose run --rm --no-deps \\
      -v dataeng_flink-checkpoints:/opt/flink/checkpoints \\
      --entrypoint python airflow-scheduler -m include.reset_data --apply

Bỏ qua bước đó cũng KHÔNG để lại state: job submit mới luôn khởi động với state
rỗng, Flink chỉ restore khi được chỉ định ``-s <savepoint>`` tường minh. File
checkpoint còn lại chỉ là rác ~1,5 MB.

Sau khi xoá, nạp lại::

    docker compose exec flink-jobmanager /opt/flink/sql/submit.sh   # job realtime
    docker compose up -d stream-generator ops-ingest feature-bridge
    # + generator/generate_offline.py cho lịch sử, rồi trigger ml_pipeline

Exit code 0 = mọi bước OK. Khác 0 = số bước FAIL.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

# Thứ tự trong tuple này LÀ thứ tự thực thi — xem ghi chú ở docstring.
STEPS = ("flink", "kafka", "postgres", "redis", "spark")

# (biến env chứa tên DB, default, schema cần dọn)
PG_TARGETS = (
    ("OPS_POSTGRES_DB", "opsdb", "ops"),
    ("WAREHOUSE_POSTGRES_DB", "warehouse", "application"),
)

TOPICS = ("transactions", "merchant_rt_10min", "device_rt_1h")

# Group phải xoá cùng lúc với record: sau khi truncate, offset đã commit của group
# trỏ ra ngoài khoảng còn tồn tại. Consumer sẽ tự lùi theo auto.offset.reset, nhưng
# `ops-ingest` đặt reset=latest -> nó bỏ qua đúng những message đầu tiên của lần nạp
# lại. Xoá group thì lần sau bắt đầu sạch. Giá trị lấy từ default của từng service.
GROUPS = ("ops-ingest", "feast-feature-bridge", "flink-realtime-features")

FLINK_REST = os.environ.get("FLINK_REST", "http://flink-jobmanager:8081")
FLINK_CHECKPOINT_DIR = os.environ.get("FLINK_CHECKPOINT_DIR", "/opt/flink/checkpoints")
SPARK_EVENTS_DIR = os.environ.get("SPARK_EVENTS_DIR", "/opt/spark-events")

TIMEOUT_S = 30

OK = "  \033[32mOK\033[0m"
NO = "  \033[31mFAIL\033[0m"
WARN = "  \033[33mBỎ QUA\033[0m"
INFO = "      "


class ResetFailed(Exception):
    """Một bước thất bại, kèm gợi ý xử lý."""

    def __init__(self, msg: str, hint: str = ""):
        super().__init__(msg)
        self.hint = hint


def _env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise ResetFailed(f"thiếu biến môi trường {name}",
                          "kiểm .env đã được mount và điền đủ chưa")
    return v


def _purge_dir(path: Path) -> tuple[int, int]:
    """Xoá NỘI DUNG của thư mục, giữ lại chính thư mục. Trả (số file, tổng byte).

    Giữ thư mục vì Flink/Spark chỉ mkdir + chown lúc init (xem ``flink-init`` trong
    docker-compose); xoá cả thư mục thì container đang chạy mất chỗ ghi và chỉ lộ ra
    ở checkpoint kế tiếp.
    """
    n = size = 0
    for child in path.iterdir():
        if child.is_dir() and not child.is_symlink():
            for f in child.rglob("*"):
                if f.is_file():
                    n += 1
                    size += f.stat().st_size
            shutil.rmtree(child)
        else:
            n += 1
            size += child.stat().st_size
            child.unlink()
    return n, size


# --------------------------------------------------------------------------- #
# Flink — cancel job + xoá checkpoint đã retain                                #
# --------------------------------------------------------------------------- #

def _flink(path: str, method: str = "GET") -> dict:
    req = urllib.request.Request(f"{FLINK_REST}{path}", method=method)
    with urllib.request.urlopen(req, timeout=TIMEOUT_S) as resp:
        body = resp.read()
    return json.loads(body) if body else {}


def reset_flink(dry: bool, force: bool) -> None:
    """Cancel mọi job đang chạy, rồi xoá checkpoint đã externalize.

    Cancel là bước THỰC SỰ reset state: job mới submit sau đó có window rỗng và đọc
    Kafka theo ``scan.startup.mode`` trong file SQL. Xoá file checkpoint chỉ là dọn
    rác — ``RETAIN_ON_CANCELLATION`` cố ý giữ chúng lại sau khi cancel.
    """
    try:
        jobs = _flink("/jobs").get("jobs", [])
    except (urllib.error.URLError, OSError, TimeoutError) as e:
        msg = f"không gọi được REST API Flink tại {FLINK_REST} ({e})"
        hint = ("Flink đang tắt thì bỏ qua bước này bằng --force (state nằm trong "
                "checkpoint, job submit mới không restore nên vẫn sạch). Chạy bằng "
                "`docker compose exec` từ ngoài network dataeng thì đặt "
                "FLINK_REST=http://127.0.0.1:8082")
        if not force:
            raise ResetFailed(msg, hint) from e
        print(f"{INFO}CẢNH BÁO: {msg}")
        print(f"{INFO}--force -> coi như không có job nào đang chạy")
        jobs = []

    live = [j for j in jobs if j.get("status") in ("RUNNING", "RESTARTING", "CREATED")]
    print(f"{INFO}job đang chạy: {len(live)}"
          + (f" ({', '.join(j['id'][:8] for j in live)})" if live else ""))

    if live and not dry:
        for j in live:
            _flink(f"/jobs/{j['id']}?mode=cancel", method="PATCH")
        # Chờ tới trạng thái cuối: xoá checkpoint khi job chưa cancel xong thì nó
        # đang ghi dở và có thể sinh lại file ngay sau khi ta xoá.
        still = live          # gán trước: TIMEOUT_S <= 0 thì vòng lặp không chạy lần nào
        deadline = time.time() + TIMEOUT_S
        while time.time() < deadline:
            still = [j for j in _flink("/jobs").get("jobs", [])
                     if j.get("status") in ("RUNNING", "RESTARTING", "CREATED",
                                            "CANCELLING")]
            if not still:
                break
            time.sleep(1)
        else:
            raise ResetFailed(
                f"{len(still)} job chưa cancel xong sau {TIMEOUT_S}s",
                "xem log flink-jobmanager; có thể taskmanager đã chết nên job treo")
        print(f"{INFO}đã cancel {len(live)} job")

    ckpt = Path(FLINK_CHECKPOINT_DIR)
    if not ckpt.is_dir():
        print(f"{INFO}{FLINK_CHECKPOINT_DIR} không có trong container này -> "
              "bỏ qua phần dọn file")
        print(f"{INFO}(không sao: job submit mới không restore từ checkpoint. Muốn "
              "dọn thì xem cách `docker compose run -v ...` ở docstring)")
        return

    if dry:
        files = [f for f in ckpt.rglob("*") if f.is_file()]
        size = sum(f.stat().st_size for f in files)
        print(f"{INFO}sẽ xoá {len(files):,} file checkpoint ({size/1e6:.1f} MB)")
        return
    n, size = _purge_dir(ckpt)
    print(f"{INFO}đã xoá {n:,} file checkpoint ({size/1e6:.1f} MB)")


# --------------------------------------------------------------------------- #
# Kafka — truncate record, giữ topic + partition + config                      #
# --------------------------------------------------------------------------- #

def _await(admin, futures: dict, timeout: float = 60.0) -> dict:
    """Chờ future của AdminClient. Trả ``key -> (value, error)``, KHÔNG raise.

    Vừa chờ vừa ``poll()``: với SASL/OAUTHBEARER, ``oauth_cb`` chỉ chạy khi hàng đợi
    sự kiện của client được phục vụ (cùng lý do ``check_connections.py`` dùng
    Consumer chứ không AdminClient). Thiếu token thì mọi future timeout mà không nói
    lý do, nên poll ở đây là bảo hiểm rẻ.

    Không raise để một topic lỗi không che mất kết quả của các topic còn lại.
    """
    out: dict = {}
    pending = dict(futures)
    deadline = time.time() + timeout
    while pending and time.time() < deadline:
        admin.poll(0.2)
        for key, fut in list(pending.items()):
            if not fut.done():
                continue
            del pending[key]
            try:
                out[key] = (fut.result(), None)
            except Exception as e:                                # noqa: BLE001
                out[key] = (None, e)
    for key in pending:
        out[key] = (None, TimeoutError(f"không có phản hồi sau {timeout:.0f}s"))
    return out


def _cleanup_policies(admin, topics) -> dict[str, str]:
    """topic -> giá trị ``cleanup.policy`` đang có."""
    from confluent_kafka.admin import ConfigResource

    res = _await(admin, admin.describe_configs(
        [ConfigResource(ConfigResource.Type.TOPIC, t) for t in topics]), TIMEOUT_S)
    out = {}
    for cr, (cfg, err) in res.items():
        if err is not None:
            raise ResetFailed(f"describe_configs({cr.name}) lỗi: {err}")
        out[cr.name] = cfg["cleanup.policy"].value
    return out


def _set_cleanup_policies(admin, policies: dict[str, str]) -> None:
    """Đặt lại ``cleanup.policy`` cho từng topic (incremental — không đụng key khác).

    Dùng ``incremental_alter_configs`` chứ KHÔNG ``alter_configs``: bản cũ ghi đè
    TOÀN BỘ config của topic, mọi key không truyền vào sẽ về default. Đó đúng là
    kiểu mất config âm thầm mà script này phải tránh.
    """
    from confluent_kafka.admin import AlterConfigOpType, ConfigEntry, ConfigResource

    resources = []
    for topic, policy in policies.items():
        cr = ConfigResource(ConfigResource.Type.TOPIC, topic)
        cr.add_incremental_config(ConfigEntry(
            "cleanup.policy", policy, incremental_operation=AlterConfigOpType.SET))
        resources.append(cr)
    res = _await(admin, admin.incremental_alter_configs(resources), TIMEOUT_S)
    errs = [f"{cr.name}: {e}" for cr, (_, e) in res.items() if e is not None]
    if errs:
        raise ResetFailed("không đặt được cleanup.policy: " + "; ".join(errs))


def reset_kafka(dry: bool) -> None:
    """``delete_records`` tới high watermark + xoá consumer group.

    ``delete_records`` là cách DUY NHẤT truncate mà giữ nguyên topic. Cách hay bị
    dùng thay thế — hạ ``retention.ms`` rồi đặt lại — phụ thuộc thời điểm broker chạy
    log cleaner nên không xác định được khi nào data thật sự biến mất.

    NHƯNG hai topic ``*_rt`` là ``cleanup.policy=compact``, và Kafka TỪ CHỐI
    DeleteRecords trên topic không có ``delete`` trong policy
    (``Partition.deleteRecordsOnLeader`` ném ``PolicyViolationException`` ->
    POLICY_VIOLATION). Nên với riêng mấy topic đó: tạm đặt ``compact,delete``,
    truncate, rồi trả policy về đúng giá trị đọc được lúc đầu — trong ``finally``,
    vì bỏ dở ở trạng thái ``compact,delete`` là topic bắt đầu tự xoá theo retention
    và sink upsert-kafka mất giá trị mới nhất của những entity im lặng.
    """
    from confluent_kafka import (OFFSET_END, KafkaError, KafkaException,
                                 TopicPartition)
    from confluent_kafka.admin import AdminClient, OffsetSpec

    try:                        # chạy như package (python -m include.reset_data)
        from include import kafka_conf
    except ImportError:        # chạy trực tiếp trong thư mục include/
        import kafka_conf

    admin = AdminClient(kafka_conf.client_config())
    md = admin.list_topics(timeout=TIMEOUT_S)

    parts: list[TopicPartition] = []
    missing = []
    for t in TOPICS:
        if t not in md.topics:
            missing.append(t)
            continue
        parts += [TopicPartition(t, p) for p in md.topics[t].partitions]
    if missing:
        raise ResetFailed(
            f"không thấy topic: {', '.join(missing)}",
            "script này chỉ xoá DATA, không tạo topic — tạo bằng "
            "`gcloud managed-kafka topics create` (xem README) rồi chạy lại")

    # Đếm trước: high - low của từng partition. Dùng cho cả dry-run lẫn để biết
    # thật sự đã xoá được gì. Hai lời gọi riêng vì mỗi partition chỉ nhận MỘT
    # OffsetSpec trong một request.
    lo = _await(admin, admin.list_offsets({tp: OffsetSpec.earliest() for tp in parts}),
                TIMEOUT_S)
    hi = _await(admin, admin.list_offsets({tp: OffsetSpec.latest() for tp in parts}),
                TIMEOUT_S)

    total = 0
    for tp in parts:
        a, b = lo.get(tp, (None, None))[0], hi.get(tp, (None, None))[0]
        if a is not None and b is not None:
            total += b.offset - a.offset
    print(f"{INFO}{len(parts)} partition / {len(TOPICS)} topic, "
          f"~{total:,} record đang có")

    # Topic nào thiếu 'delete' trong policy thì DeleteRecords bị từ chối -> phải nới
    # tạm. Đọc giá trị THẬT chứ không giả định 'compact': nếu ai đó đổi config trên
    # broker thì phải trả về đúng cái họ đang dùng.
    policies = _cleanup_policies(admin, TOPICS)
    need_relax = {t: p for t, p in policies.items() if "delete" not in p}
    for t, p in policies.items():
        print(f"{INFO}  {t}: cleanup.policy={p}"
              + (f" -> tạm '{p},delete'" if t in need_relax else ""))

    if dry:
        print(f"{INFO}sẽ xoá record tới high watermark, giữ topic + partition + config")
        print(f"{INFO}sẽ xoá consumer group: {', '.join(GROUPS)}")
        return

    if need_relax:
        _set_cleanup_policies(admin, {t: f"{p},delete" for t, p in need_relax.items()})
    try:
        # OFFSET_END (-1) = "xoá tới high watermark", broker tự resolve từng partition.
        res = _await(admin, admin.delete_records(
            [TopicPartition(tp.topic, tp.partition, OFFSET_END) for tp in parts]),
            TIMEOUT_S * 2)
    finally:
        if need_relax:
            _set_cleanup_policies(admin, need_relax)
            back = _cleanup_policies(admin, list(need_relax))
            # Kiểm lại bằng cách ĐỌC từ broker, không tin lời alter_configs: sai ở
            # đây là topic *_rt phình vô hạn hoặc mất giá trị mới nhất, mà cả hai
            # đều chỉ lộ ra nhiều ngày sau.
            drift = {t: back[t] for t in need_relax if back[t] != need_relax[t]}
            if drift:
                raise ResetFailed(
                    f"cleanup.policy KHÔNG trả về được như cũ: {drift} "
                    f"(mong đợi {need_relax})",
                    "đặt lại bằng tay: gcloud managed-kafka topics update <topic> "
                    "--configs=cleanup.policy=compact")
            print(f"{INFO}  đã trả cleanup.policy về: "
                  + ", ".join(f"{t}={p}" for t, p in back.items()))

    errs = [f"{tp.topic}[{tp.partition}]: {e}" for tp, (_, e) in res.items() if e]
    if errs:
        raise ResetFailed("delete_records lỗi: " + "; ".join(errs),
                          "SA có roles/managedkafka.client chưa? topic có bị ACL "
                          "chặn DeleteRecords không?")
    print(f"{INFO}đã truncate {len(res)} partition (~{total:,} record)")

    # Group không tồn tại (chưa chạy lần nào, hoặc đã xoá) -> coi là sạch. Group còn
    # member (service chưa stop) -> lỗi thật, phải báo. So MÃ lỗi chứ không so chuỗi:
    # cùng một tình huống có ít nhất hai mã (broker trả GROUP_ID_NOT_FOUND, client
    # tự trả _UNKNOWN_GROUP khi không tìm được coordinator), và chuỗi thì đổi theo
    # phiên bản librdkafka.
    absent = {getattr(KafkaError, n) for n in ("GROUP_ID_NOT_FOUND", "_UNKNOWN_GROUP")
              if hasattr(KafkaError, n)}
    gres = _await(admin, admin.delete_consumer_groups(list(GROUPS)), TIMEOUT_S)
    gone, kept = [], []
    for g, (_, e) in gres.items():
        code = e.args[0].code() if isinstance(e, KafkaException) and e.args else None
        if e is None:
            gone.append(g)
        elif code in absent:
            gone.append(f"{g} (chưa từng có)")
        else:
            kept.append(f"{g}: {e}")
    print(f"{INFO}group đã xoá: {', '.join(gone) or 'không có'}")
    if kept:
        raise ResetFailed("không xoá được group: " + "; ".join(kept),
                          "group còn member thì không xoá được — stop "
                          "stream-generator/ops-ingest/feature-bridge và cancel job "
                          "Flink trước (bước flink của script này)")


# --------------------------------------------------------------------------- #
# Postgres — TRUNCATE, giữ database + schema + bảng                             #
# --------------------------------------------------------------------------- #

def reset_postgres(dry: bool) -> None:
    """TRUNCATE mọi bảng trong ``ops`` (opsdb) và ``application`` (warehouse).

    Lấy danh sách bảng từ ``information_schema`` chứ không hardcode: ``feat_*`` do
    Spark tạo bằng ``mode("overwrite")`` nên tập bảng thay đổi theo job, hardcode là
    bỏ sót. TRUNCATE (không DROP) nên bảng, index và quyền còn nguyên.
    """
    import psycopg
    from psycopg import sql

    host, port = _env("PG_HOST"), os.environ.get("POSTGRES_PORT", "5432")
    user = os.environ.get("POSTGRES_USER") or _env("AIRFLOW_USER")
    password = os.environ.get("POSTGRES_PASSWORD") or _env("AIRFLOW_PASSWORD")

    for env_name, default_db, schema in PG_TARGETS:
        db = os.environ.get(env_name, default_db)
        dsn = (f"host={host} port={port} dbname={db} user={user} "
               f"password={password} connect_timeout={TIMEOUT_S}")
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s AND table_type = 'BASE TABLE' "
                "ORDER BY table_name", (schema,))
            tables = [r[0] for r in cur.fetchall()]
            if not tables:
                print(f"{INFO}{db}.{schema}: không có bảng nào -> bỏ qua")
                continue

            counts = {}
            for t in tables:
                cur.execute(sql.SQL("SELECT count(*) FROM {}.{}").format(
                    sql.Identifier(schema), sql.Identifier(t)))
                counts[t] = cur.fetchone()[0]
            total = sum(counts.values())
            detail = ", ".join(f"{t}={n:,}" for t, n in counts.items())
            print(f"{INFO}{db}.{schema}: {len(tables)} bảng, {total:,} dòng")
            print(f"{INFO}  {detail}")

            if dry:
                continue
            if total == 0:
                print(f"{INFO}  đã rỗng, không cần TRUNCATE")
                continue
            # MỘT câu TRUNCATE cho cả schema: nếu sau này có FK giữa các bảng trong
            # schema thì cách này vẫn chạy, còn truncate lần lượt sẽ vỡ. Không dùng
            # CASCADE — FK trỏ từ bảng NGOÀI tập này thì phải nổ để ta biết.
            cur.execute(sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY").format(
                sql.SQL(", ").join(
                    sql.Identifier(schema, t) for t in tables)))
            conn.commit()
            print(f"{INFO}  đã TRUNCATE {len(tables)} bảng ({total:,} dòng)")


# --------------------------------------------------------------------------- #
# Redis — FLUSHDB (online store của Feast)                                     #
# --------------------------------------------------------------------------- #

def reset_redis(dry: bool) -> None:
    """FLUSHDB trên đúng DB mà Feast dùng.

    Redis không có "structure" để giữ: key của online store do Feast sinh (entity
    key đã serialize), không có schema nào tồn tại độc lập với data. Nên FLUSHDB —
    không phải FLUSHALL, để không đụng DB khác trên cùng instance.
    """
    import redis

    host = _env("REDIS_HOST")
    port = int(os.environ.get("REDIS_PORT", "6379"))
    db = int(os.environ.get("REDIS_DB", "0"))
    r = redis.Redis(host=host, port=port, db=db,
                    socket_timeout=TIMEOUT_S, socket_connect_timeout=TIMEOUT_S)
    try:
        r.ping()
        n = r.dbsize()
        print(f"{INFO}{host}:{port} db={db}, DBSIZE={n:,}")
        if dry:
            print(f"{INFO}sẽ FLUSHDB (xoá {n:,} key)")
            return
        r.flushdb()
        print(f"{INFO}đã FLUSHDB, DBSIZE={r.dbsize():,}")
    except redis.RedisError as e:
        raise ResetFailed(f"Redis lỗi: {e}",
                          "VM cùng VPC với Memorystore chưa? instance có read-only?"
                          ) from e


# --------------------------------------------------------------------------- #
# Spark — event log của history server                                         #
# --------------------------------------------------------------------------- #

def reset_spark(dry: bool) -> None:
    """Xoá event log. Spark không giữ state nào khác.

    Output của DP2/DP3 nằm ở lake và ở ``application.feat_*`` — hai chỗ đó do bước
    khác (và tay bạn, với Cloud Storage) dọn. Event log chỉ là lịch sử quan sát trên
    UI 18080, xoá đi không ảnh hưởng lần chạy sau.
    """
    d = Path(SPARK_EVENTS_DIR)
    if not d.is_dir():
        print(f"{INFO}{SPARK_EVENTS_DIR} không có trong container này -> bỏ qua")
        print(f"{INFO}(volume spark-events được mount ở service airflow-*; chạy "
              "script trong đó thì bước này mới làm được)")
        return
    files = [f for f in d.rglob("*") if f.is_file()]
    size = sum(f.stat().st_size for f in files)
    print(f"{INFO}{len(files):,} file event log ({size/1e6:.1f} MB)")
    if dry:
        print(f"{INFO}sẽ xoá toàn bộ, giữ lại thư mục")
        return
    n, size = _purge_dir(d)
    print(f"{INFO}đã xoá {n:,} file ({size/1e6:.1f} MB)")


# --------------------------------------------------------------------------- #

RUNNERS = {
    "flink": lambda dry, force: reset_flink(dry, force),
    "kafka": lambda dry, force: reset_kafka(dry),
    "postgres": lambda dry, force: reset_postgres(dry),
    "redis": lambda dry, force: reset_redis(dry),
    "spark": lambda dry, force: reset_spark(dry),
}


def _confirm(steps: tuple[str, ...]) -> bool:
    """Bắt gõ tay chữ RESET. Không có đường lùi nào sau TRUNCATE/FLUSHDB."""
    print("\n\033[31mXOÁ THẬT\033[0m — các bước: " + ", ".join(steps))
    print("Không thể hoàn tác. Gõ RESET để tiếp tục (Enter để thoát): ", end="")
    try:
        return input().strip() == "RESET"
    except EOFError:
        print("\nstdin không tương tác (docker compose ... -T?) -> dùng --yes")
        return False


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--apply", action="store_true",
                   help="xoá thật. Không có cờ này thì chỉ in ra sẽ xoá gì")
    p.add_argument("--yes", action="store_true",
                   help="bỏ qua bước gõ tay xác nhận (dùng cho chạy tự động)")
    p.add_argument("--only", default=",".join(STEPS),
                   help=f"chỉ chạy một số bước, phân tách bằng dấu phẩy: {','.join(STEPS)}")
    p.add_argument("--force", action="store_true",
                   help="Flink không phản hồi thì cảnh báo rồi đi tiếp thay vì fail")
    args = p.parse_args()

    steps = tuple(s.strip() for s in args.only.split(",") if s.strip())
    unknown = [s for s in steps if s not in RUNNERS]
    if unknown:
        p.error(f"bước không tồn tại: {', '.join(unknown)} "
                f"(có: {', '.join(STEPS)})")
    # Chạy theo thứ tự trong STEPS, không theo thứ tự người dùng gõ: cắt nguồn ghi
    # phải xong trước khi xoá store hạ nguồn.
    steps = tuple(s for s in STEPS if s in steps)

    dry = not args.apply
    print(f"=== reset_data: {'DRY-RUN (không xoá gì)' if dry else 'XOÁ THẬT'} ===")
    if not dry and not args.yes and not _confirm(steps):
        print("Đã huỷ, không xoá gì.")
        return 1

    failed = 0
    for name in steps:
        print(f"\n[{name}]")
        try:
            RUNNERS[name](dry, args.force)
        except ResetFailed as e:
            failed += 1
            print(f"{NO} {e}")
            if e.hint:
                print(f"{INFO}-> {e.hint}")
        except Exception as e:                                    # noqa: BLE001
            failed += 1
            print(f"{NO} {type(e).__name__}: {e}")
        else:
            print(f"{OK}")

    print(f"\n=== {len(steps) - failed}/{len(steps)} bước OK ===")
    if dry:
        print("Chạy lại với --apply để xoá thật.")
    elif not failed:
        print("Nạp lại: submit.sh cho job Flink, `docker compose up -d` 3 service "
              "streaming, rồi generate_offline.py + trigger ml_pipeline.")
    return failed


if __name__ == "__main__":
    sys.exit(main())
