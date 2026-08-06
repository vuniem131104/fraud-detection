"""Smoke test kết nối tới mọi dịch vụ managed — CHẠY TRƯỚC KHI DEPLOY.

Kiểm đúng những thứ mà pipeline sẽ dùng, theo thứ tự dễ sai nhất trước. Mỗi check
độc lập: một cái fail thì các cái sau vẫn chạy, nên một lần chạy thấy hết vấn đề.

Chạy trên VM, TRƯỚC KHI `docker compose up` (chỉ cần image đã build)::

    docker compose build
    docker run --rm --env-file .env -v "$PWD:/repo" -w /repo \\
      --entrypoint python fraud-detection/airflow:3.2.2 check_connections.py

    # chỉ một phần
    ... check_connections.py --only postgres,kafka

Exit code 0 = tất cả PASS. Khác 0 = số check FAIL.

KHÔNG in secret: chỉ in host/port/tên topic, không in password hay token.
"""

from __future__ import annotations

import argparse
import os
import socket
import sys
import time
from pathlib import Path

# lake.py và kafka_conf.py nằm trong airflow/include (nơi container mount thành
# /opt/airflow/code/include). Script này chạy từ gốc repo nên phải tự thêm path.
sys.path.insert(0, str(Path(__file__).resolve().parent / "airflow" / "include"))

# Timeout ngắn: mục đích là biết SAI Ở ĐÂU nhanh, không phải chờ retry.
TIMEOUT_S = 10

OK = "  \033[32mPASS\033[0m"
NO = "  \033[31mFAIL\033[0m"
INFO = "      "


class CheckFailed(Exception):
    """Check thất bại kèm gợi ý xử lý."""

    def __init__(self, msg: str, hint: str = ""):
        super().__init__(msg)
        self.hint = hint


def _env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise CheckFailed(f"thiếu biến môi trường {name}",
                          "kiểm .env đã được mount và điền đủ chưa")
    return v


def _tcp(host: str, port: int, what: str) -> None:
    """Kiểm tầng TCP trước khi thử protocol.

    Tách riêng vì hai lỗi này cần cách xử lý khác nhau: TCP fail = sai VPC /
    firewall / IP; TCP ok nhưng protocol fail = sai credential hoặc TLS.
    """
    try:
        with socket.create_connection((host, port), timeout=TIMEOUT_S):
            pass
    except OSError as e:
        raise CheckFailed(
            f"không mở được TCP tới {host}:{port} ({e})",
            f"VM có cùng VPC với {what} không? {what} đã bật Private IP chưa? "
            "egress TCP có bị chặn không?",
        ) from e


# --------------------------------------------------------------------------- #
# Postgres (Cloud SQL, Private IP)                                            #
# --------------------------------------------------------------------------- #

def check_postgres() -> None:
    """Kết nối 4 database + kiểm schema và bảng mà DDL phải đã tạo."""
    import psycopg

    host = _env("PG_HOST")
    port = int(os.environ.get("POSTGRES_PORT", "5432"))
    user = os.environ.get("POSTGRES_USER") or _env("AIRFLOW_USER")
    password = os.environ.get("POSTGRES_PASSWORD") or _env("AIRFLOW_PASSWORD")

    _tcp(host, port, "Cloud SQL")
    print(f"{INFO}TCP {host}:{port} ok")

    dbs = {
        "airflow": os.environ.get("AIRFLOW_DB", "airflow"),
        "ops": os.environ.get("OPS_POSTGRES_DB", "opsdb"),
        "warehouse": os.environ.get("WAREHOUSE_POSTGRES_DB", "warehouse"),
        "feast registry": os.environ.get("FEAST_REGISTRY_DB", "feast-registry"),
    }
    for label, db in dbs.items():
        dsn = (f"host={host} port={port} dbname={db} user={user} "
               f"password={password} connect_timeout={TIMEOUT_S}")
        try:
            with psycopg.connect(dsn) as conn:
                ver = conn.execute("SELECT version()").fetchone()[0]
        except psycopg.OperationalError as e:
            msg = str(e).strip().splitlines()[0]
            hint = f"database '{db}' đã tạo chưa? user/password đúng chưa?"
            if "SSL" in str(e) or "ssl" in str(e):
                # Instance bật Enforce SSL nhưng code không set sslmode.
                hint = ("instance đang bật Enforce SSL. Tắt tuỳ chọn đó, hoặc thêm "
                        "sslmode=require vào 3 hàm dựng DSN "
                        "(ops_to_source.ops_dsn, kafka_to_ops.ops_dsn, "
                        "ops_store.pg_dsn)")
            raise CheckFailed(f"{db}: {msg}", hint) from e
        print(f"{INFO}{label:<14} {db:<15} ok  ({ver.split(',')[0]})")

    # DDL đã apply chưa — thiếu là DP0 chết giữa pipeline chứ không phải lúc `up`.
    checks = [
        (dbs["ops"], "ops", ["transactions", "users", "cards", "merchants", "devices"]),
        (dbs["warehouse"], "application",
         ["labels", "feat_card", "feat_user", "feat_merchant", "feat_device",
          "feat_merchant_rt", "feat_device_rt"]),
    ]
    for db, schema, tables in checks:
        dsn = (f"host={host} port={port} dbname={db} user={user} "
               f"password={password} connect_timeout={TIMEOUT_S}")
        with psycopg.connect(dsn) as conn:
            found = {r[0] for r in conn.execute(
                "SELECT tablename FROM pg_tables WHERE schemaname = %s", (schema,))}
        missing = [t for t in tables if t not in found]
        if missing:
            raise CheckFailed(
                f"{db}.{schema}: thiếu bảng {', '.join(missing)}",
                "chưa apply DDL trong sql/ — xem README §3.5",
            )
        print(f"{INFO}{db}.{schema}: đủ {len(tables)} bảng")


# --------------------------------------------------------------------------- #
# Redis (Memorystore)                                                         #
# --------------------------------------------------------------------------- #

def check_redis() -> None:
    """PING + một vòng ghi/đọc/xoá — chứng minh có quyền WRITE, không chỉ đọc."""
    import redis

    host = _env("REDIS_HOST")
    port = int(os.environ.get("REDIS_PORT", "6379"))
    _tcp(host, port, "Memorystore")
    print(f"{INFO}TCP {host}:{port} ok")

    r = redis.Redis(host=host, port=port, db=int(os.environ.get("REDIS_DB", "0")),
                    socket_timeout=TIMEOUT_S, socket_connect_timeout=TIMEOUT_S)
    try:
        r.ping()
    except redis.RedisError as e:
        raise CheckFailed(f"PING lỗi: {e}",
                          "Memorystore có bật AUTH không? cùng VPC chưa?") from e

    key = "_healthcheck:check_connections"
    try:
        r.setex(key, 60, "ok")
        got = r.get(key)
        r.delete(key)
    except redis.RedisError as e:
        raise CheckFailed(f"ghi/đọc lỗi: {e}", "instance có ở chế độ read-only?") from e
    if got != b"ok":
        raise CheckFailed(f"đọc lại không khớp: {got!r}")

    n = r.dbsize()
    print(f"{INFO}PING ok, ghi/đọc/xoá ok, DBSIZE={n:,}")
    if n == 0:
        print(f"{INFO}(rỗng là bình thường nếu chưa chạy feast materialize)")


# --------------------------------------------------------------------------- #
# Kafka (Managed Service for Apache Kafka, SASL_SSL/OAUTHBEARER)               #
# --------------------------------------------------------------------------- #

REQUIRED_TOPICS = {
    "transactions": None,          # không yêu cầu cleanup.policy
    "merchant_rt_10min": "compact",
    "device_rt_1h": "compact",
}


def check_kafka() -> None:
    """Lấy metadata + kiểm 3 topic, và cleanup.policy của 2 topic *_rt."""
    from confluent_kafka import Consumer, KafkaException

    import kafka_conf

    bootstrap = kafka_conf.bootstrap()
    host, _, port = bootstrap.rpartition(":")
    _tcp(host, int(port), "Managed Kafka")
    print(f"{INFO}TCP {host}:{port} ok")
    print(f"{INFO}{kafka_conf.describe()}")

    # Dùng Consumer chứ không AdminClient: với SASL/OAUTHBEARER, callback lấy token
    # chỉ được gọi trong poll(), mà AdminClient không expose poll().
    cfg = kafka_conf.client_config(**{
        "group.id": "_healthcheck",
        "socket.timeout.ms": TIMEOUT_S * 1000,
    })
    consumer = Consumer(cfg)
    try:
        # Vài nhịp poll ngắn để oauth_cb kịp chạy và lấy access token.
        deadline = time.time() + TIMEOUT_S
        while time.time() < deadline:
            consumer.poll(0.5)
            break
        try:
            md = consumer.list_topics(timeout=TIMEOUT_S * 2)
        except KafkaException as e:
            raise CheckFailed(
                f"không lấy được metadata: {e}",
                "Nếu log có 'invalid credentials with SASL mechanism OAUTHBEARER' "
                "thì vấn đề là TOKEN, không phải mạng: oauth_cb phải trả 4-tuple "
                "(token, expiry_epoch_giây, principal, extensions) — principal là "
                "email SA và không được rỗng. Nếu không có dòng đó: SA có "
                "roles/managedkafka.client chưa? VM có scope cloud-platform chưa?",
            ) from e

        print(f"{INFO}broker: {len(md.brokers)}  topic: {len(md.topics)}")
        missing = [t for t in REQUIRED_TOPICS if t not in md.topics]
        if missing:
            raise CheckFailed(
                f"thiếu topic: {', '.join(missing)}",
                "tạo bằng `gcloud managed-kafka topics create ...` — xem README §3.2",
            )
        for t, policy in REQUIRED_TOPICS.items():
            parts = len(md.topics[t].partitions)
            note = f"  (cần cleanup.policy={policy})" if policy else ""
            print(f"{INFO}topic {t:<20} {parts} partition{note}")
    finally:
        consumer.close()

    # cleanup.policy phải là compact cho 2 topic sink của Flink: không compact thì
    # topic phình vô hạn (mỗi giao dịch sinh 10 row cho merchant).
    _check_topic_configs(cfg)


def _check_topic_configs(cfg: dict) -> None:
    """Đọc cleanup.policy của 2 topic *_rt. Chỉ cảnh báo nếu API không dùng được."""
    try:
        from confluent_kafka.admin import AdminClient, ConfigResource
    except ImportError:
        print(f"{INFO}(bỏ qua kiểm cleanup.policy: confluent_kafka.admin không có)")
        return
    admin = AdminClient(cfg)
    want = {t: p for t, p in REQUIRED_TOPICS.items() if p}
    resources = [ConfigResource(ConfigResource.Type.TOPIC, t) for t in want]
    try:
        futures = admin.describe_configs(resources, request_timeout=TIMEOUT_S)
    except Exception as e:                                    # noqa: BLE001
        print(f"{INFO}(không đọc được config topic: {e})")
        return
    wrong = []
    for res, fut in futures.items():
        try:
            entries = fut.result(timeout=TIMEOUT_S)
        except Exception as e:                                # noqa: BLE001
            print(f"{INFO}(không đọc được config {res.name}: {e})")
            continue
        actual = entries["cleanup.policy"].value if "cleanup.policy" in entries else "?"
        expect = want[res.name]
        mark = "ok" if expect in str(actual) else "SAI"
        print(f"{INFO}{res.name:<20} cleanup.policy={actual}  {mark}")
        if expect not in str(actual):
            wrong.append(res.name)
    if wrong:
        raise CheckFailed(
            f"cleanup.policy không phải compact: {', '.join(wrong)}",
            "sink upsert-kafka của Flink dựa vào compaction; không compact thì "
            "topic phình vô hạn. Sửa bằng `gcloud managed-kafka topics update`",
        )


# --------------------------------------------------------------------------- #
# Cloud Storage — không phải yêu cầu ban đầu, nhưng là chỗ fail hay gặp nhất   #
# --------------------------------------------------------------------------- #

def check_gcs() -> None:
    """List + ghi/xoá một object nhỏ trong data lake.

    Đây là check phát hiện lỗi SCOPE: VM tạo với scope mặc định có
    ``devstorage.read_only`` nên list thì được mà ghi thì fail — và DP0 sẽ chết
    đúng ở bước ghi, sau khi mọi thứ khác trông như đã ổn.
    """
    import pyarrow.fs as pafs

    import lake

    try:
        root = lake.lake_root()
    except RuntimeError as e:
        raise CheckFailed(str(e), "đặt LAKE_ROOT trong .env, ví dụ gs://my-lake/") from e
    fs = lake.filesystem()
    probe = lake.path("_healthcheck", "check_connections.txt")
    print(f"{INFO}LAKE_ROOT {root}")

    try:
        fs.get_file_info(pafs.FileSelector(lake.path("").rstrip("/"), recursive=False))
    except Exception as e:                                    # noqa: BLE001
        raise CheckFailed(f"không list được {root}: {e}",
                          "bucket đã tạo chưa? SA có roles/storage.objectAdmin?") from e
    print(f"{INFO}list ok")

    try:
        with fs.open_output_stream(probe) as f:
            f.write(b"ok\n")
        with fs.open_input_stream(probe) as f:
            data = f.read()
        fs.delete_file(probe)
    except Exception as e:                                    # noqa: BLE001
        raise CheckFailed(
            f"không ghi được vào {root}: {e}",
            "VM có scope cloud-platform chưa? Scope mặc định là "
            "devstorage.read_only -> list được nhưng KHÔNG ghi được, và IAM đúng "
            "cũng không cứu được. Sửa: gcloud compute instances set-service-account "
            "<vm> --scopes=https://www.googleapis.com/auth/cloud-platform (VM phải stop)",
        ) from e
    if data != b"ok\n":
        raise CheckFailed(f"đọc lại không khớp: {data!r}")
    print(f"{INFO}ghi/đọc/xoá ok")


def check_service_account() -> None:
    """Metadata server: SA nào đang gắn và có scope gì."""
    import urllib.error
    import urllib.request

    base = ("http://metadata.google.internal/computeMetadata/v1/instance/"
            "service-accounts/default/")
    req = lambda p: urllib.request.urlopen(          # noqa: E731
        urllib.request.Request(base + p, headers={"Metadata-Flavor": "Google"}),
        timeout=TIMEOUT_S).read().decode().strip()
    try:
        email = req("email")
        scopes = req("scopes").split()
    except (urllib.error.URLError, OSError) as e:
        raise CheckFailed(f"không đọc được metadata server: {e}",
                          "script này phải chạy TRÊN VM GCP") from e

    print(f"{INFO}SA: {email}")
    full = "https://www.googleapis.com/auth/cloud-platform"
    if full in scopes:
        print(f"{INFO}scope: cloud-platform (đủ)")
        return
    print(f"{INFO}scope: {' '.join(s.rsplit('/', 1)[-1] for s in scopes)}")
    raise CheckFailed(
        "VM không có scope cloud-platform",
        "Scope là lớp chặn nằm TRƯỚC IAM: thiếu nó thì GCS/Dataproc/Kafka bị chặn "
        "dù role đã đúng. Sửa: stop VM rồi "
        "`gcloud compute instances set-service-account <vm> --zone=<z> "
        f"--service-account=<sa> --scopes={full}`",
    )


CHECKS = {
    "sa": ("Service account + scope", check_service_account),
    "postgres": ("Cloud SQL (Postgres)", check_postgres),
    "redis": ("Memorystore (Redis)", check_redis),
    "kafka": ("Managed Kafka", check_kafka),
    "gcs": ("Cloud Storage", check_gcs),
}


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--only", default="",
                   help=f"danh sách check, phân cách bằng dấu phẩy ({','.join(CHECKS)})")
    args = p.parse_args()

    selected = [k.strip() for k in args.only.split(",") if k.strip()] or list(CHECKS)
    unknown = [k for k in selected if k not in CHECKS]
    if unknown:
        print(f"check không tồn tại: {', '.join(unknown)}", file=sys.stderr)
        return 2

    failed = []
    for key in selected:
        label, fn = CHECKS[key]
        print(f"\n[{key}] {label}")
        try:
            fn()
        except CheckFailed as e:
            print(f"{NO} {e}")
            if e.hint:
                print(f"{INFO}-> {e.hint}")
            failed.append(key)
        except Exception as e:                                # noqa: BLE001
            print(f"{NO} lỗi không lường trước: {type(e).__name__}: {e}")
            failed.append(key)
        else:
            print(OK)

    print("\n" + "=" * 60)
    if failed:
        print(f"{len(failed)}/{len(selected)} FAIL: {', '.join(failed)}")
        return len(failed)
    print(f"{len(selected)}/{len(selected)} PASS — sẵn sàng deploy")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
