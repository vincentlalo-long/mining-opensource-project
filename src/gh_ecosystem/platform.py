"""Local k3d platform doctor and lifecycle commands."""

import argparse
import json
import os
import re
import secrets
import shutil
import socket
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from gh_ecosystem.contracts import validate_contract
from gh_ecosystem.ingest import load_config


ROOT = Path(__file__).resolve().parents[2]
CLUSTER = "gh"
NAMESPACE = "gh"
SPARK_IMAGE = "gh-spark:3.5.5"
DEFAULT_HOUR = "2024-01-01-0"
REQUIRED_PORTS = (9000, 8080, 8088)
MIN_DISK_BYTES = 10 * 1024**3
MIN_MEMORY_BYTES = 4 * 1024**3


class CheckError(RuntimeError):
    pass


def run(command, *, check=True, capture=False, input_bytes=None, env=None):
    printable = " ".join(command)
    print(f"+ {printable}", flush=True)
    result = subprocess.run(
        command,
        cwd=ROOT,
        check=False,
        text=not input_bytes,
        input=input_bytes,
        capture_output=capture,
        env=env,
    )
    if check and result.returncode:
        detail = (result.stderr or result.stdout or "").strip()
        raise CheckError(f"command failed ({result.returncode}): {printable}\n{detail}")
    return result


def command_exists(name):
    return shutil.which(name) is not None


def port_is_free(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


def available_memory_bytes():
    for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) * 1024
    return 0


def ensure_local_config():
    local = ROOT / "config/local.yml"
    if local.exists():
        return local
    example = ROOT / "config/contract-local.example.yml"
    shutil.copyfile(example, local)
    print(f"created {local.relative_to(ROOT)} from {example.relative_to(ROOT)}")
    return local


def config_parity():
    contract = load_config(ROOT / "src/gh_ecosystem/event_contract_v1.json")
    dev = load_config(ROOT / "config/dev.yml")
    local = load_config(ROOT / "config/local.yml")
    validate_contract(contract, dev, local)


def ensure_secret(name, values):
    if kubectl("get", "secret", name, check=False, capture=True).returncode == 0:
        return
    manifest = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": name, "namespace": NAMESPACE},
        "type": "Opaque",
        "stringData": values,
    }
    run(["kubectl", "-n", NAMESPACE, "apply", "-f", "-"],
        input_bytes=(json.dumps(manifest) + "\n").encode())


def doctor(_config):
    checks = []

    def check(name, action):
        try:
            detail = action()
        except Exception as error:
            checks.append((name, False, str(error)))
        else:
            checks.append((name, True, detail or "ok"))

    check("docker", lambda: run(["docker", "info"], capture=True).stdout.splitlines()[0])
    check("k3d", lambda: run(["k3d", "version"], capture=True).stdout.splitlines()[0])
    check("kubectl", lambda: run(["kubectl", "version", "--client"], capture=True).stdout.splitlines()[0])
    for port in REQUIRED_PORTS:
        check(f"port {port}", lambda port=port: "free" if port_is_free(port) else (_ for _ in ()).throw(
            CheckError(f"port {port} is already in use")))
    check("disk", lambda: f"{shutil.disk_usage(ROOT).free / 1024**3:.1f} GiB free" if shutil.disk_usage(ROOT).free >= MIN_DISK_BYTES
          else (_ for _ in ()).throw(CheckError(f"need at least {MIN_DISK_BYTES / 1024**3:.0f} GiB free")))
    check("memory", lambda: f"{available_memory_bytes() / 1024**3:.1f} GiB available" if available_memory_bytes() >= MIN_MEMORY_BYTES
          else (_ for _ in ()).throw(CheckError(f"need at least {MIN_MEMORY_BYTES / 1024**3:.0f} GiB available")))
    check("config parity", config_parity)

    failed = False
    for name, ok, detail in checks:
        print(f"{'OK' if ok else 'FAIL':<4} {name:<14} {detail}")
        failed = failed or not ok
    return not failed


def cluster_exists():
    result = run(["k3d", "cluster", "list", CLUSTER], check=False, capture=True)
    return result.returncode == 0 and CLUSTER in result.stdout


def kubectl(*args, **kwargs):
    return run(["kubectl", "-n", NAMESPACE, *args], **kwargs)


def wait_rollout(name, timeout="240s"):
    kubectl("rollout", "status", f"deployment/{name}", f"--timeout={timeout}")


def wait_job(name, timeout="240s"):
    kubectl("wait", "--for=condition=complete", f"job/{name}", f"--timeout={timeout}")


def upload_smoke_fixture(hour, config):
    raw = ROOT / f"data/raw/{hour}.json.gz"
    if not raw.exists():
        run([sys.executable, "-m", "gh_ecosystem.ingest", "--hour", hour, "--config", str(config)])
    pod = kubectl("get", "pod", "-l", "app=minio", "-o", "jsonpath={.items[0].metadata.name}",
                  capture=True).stdout.strip()
    run(["kubectl", "-n", NAMESPACE, "exec", "-i", pod, "--", "sh", "-c", f"cat > /tmp/{hour}.json.gz"],
        input_bytes=raw.read_bytes())
    kubectl("exec", pod, "--", "sh", "-c",
            f'mc alias set local http://127.0.0.1:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" && '
            f'mc cp /tmp/{hour}.json.gz local/raw/{hour}.json.gz')
    kubectl("exec", pod, "--", "sh", "-c",
            'mc alias set local http://127.0.0.1:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" && '
            'mc ls --json local')
    downloaded = kubectl("exec", pod, "--", "sh", "-c",
                         'mc alias set local http://127.0.0.1:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && '
                         f'mc cat local/raw/{hour}.json.gz | wc -c', capture=True).stdout.strip()
    if not downloaded.isdigit() or int(downloaded) != raw.stat().st_size:
        raise CheckError(f"MinIO download size mismatch: local={raw.stat().st_size}, remote={downloaded!r}")


def read_buckets():
    pod = kubectl("get", "pod", "-l", "app=minio", "-o", "jsonpath={.items[0].metadata.name}",
                  capture=True).stdout.strip()
    output = kubectl("exec", pod, "--", "sh", "-c",
                     'mc alias set local http://127.0.0.1:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && '
                     'mc ls --json local', capture=True).stdout
    buckets = []
    for line in output.splitlines():
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if item.get("type") == "folder" and item.get("key"):
            buckets.append(item["key"].rstrip("/"))
    return sorted(buckets)


def trino_count():
    output = kubectl("exec", "deploy/trino", "--", "trino", "--execute",
                     "SELECT count(*) FROM tpch.tiny.nation", capture=True).stdout
    numbers = re.findall(r"\d+", output)
    if not numbers:
        raise CheckError(f"Trino returned no count: {output!r}")
    return int(numbers[-1])


def superset_health():
    output = kubectl("exec", "deploy/superset", "--", "python", "-c",
                     "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:8088/health', timeout=10).read().decode())",
                     capture=True).stdout.strip()
    if not output:
        raise CheckError("Superset health returned an empty response")
    return output


def node_status():
    output = run(["kubectl", "get", "nodes", "-o", "json"], capture=True).stdout
    data = json.loads(output)
    nodes = []
    for item in data["items"]:
        conditions = {condition["type"]: condition["status"] for condition in item["status"]["conditions"]}
        nodes.append({"name": item["metadata"]["name"], "ready": conditions.get("Ready") == "True"})
    return nodes


def up(config, hour):
    ensure_local_config()
    if not doctor(config):
        return 1
    if not cluster_exists():
        run(["k3d", "cluster", "create", "--config", str(ROOT / "k8s/k3d-config.yaml")])
    run(["docker", "build", "-t", SPARK_IMAGE, "-f", str(ROOT / "k8s/spark/Dockerfile"), str(ROOT / "k8s/spark")])
    run(["k3d", "image", "import", SPARK_IMAGE, "-c", CLUSTER])

    run(["kubectl", "apply", "-f", str(ROOT / "k8s/namespace.yaml")])
    ensure_secret("minio-credentials", {
        "MINIO_ROOT_USER": os.environ.get("MINIO_ROOT_USER", "admin"),
        "MINIO_ROOT_PASSWORD": os.environ.get("MINIO_ROOT_PASSWORD", secrets.token_urlsafe(18)),
    })
    run(["kubectl", "-n", NAMESPACE, "apply", "-f", str(ROOT / "k8s/minio/minio.yaml")])
    wait_rollout("minio")
    kubectl("delete", "job", "minio-init", "--ignore-not-found=true")
    run(["kubectl", "-n", NAMESPACE, "apply", "-f", str(ROOT / "k8s/minio/init-buckets-job.yaml")])
    wait_job("minio-init")

    run(["kubectl", "-n", NAMESPACE, "apply", "-f", str(ROOT / "k8s/spark/spark.yaml")])
    wait_rollout("spark-master")
    wait_rollout("spark-worker")
    run(["kubectl", "-n", NAMESPACE, "apply", "-f", str(ROOT / "k8s/trino/trino.yaml")])
    wait_rollout("trino", "300s")
    ensure_secret("superset-secret", {
        "SUPERSET_SECRET_KEY": os.environ.get("SUPERSET_SECRET_KEY", secrets.token_urlsafe(32)),
        "SUPERSET_ADMIN_PASSWORD": os.environ.get("SUPERSET_ADMIN_PASSWORD", secrets.token_urlsafe(18)),
    })
    run(["kubectl", "-n", NAMESPACE, "apply", "-f", str(ROOT / "k8s/superset/superset.yaml")])
    wait_rollout("superset", "600s")

    upload_smoke_fixture(hour, config)
    kubectl("delete", "job", "spark-smoke", "--ignore-not-found=true")
    run(["kubectl", "-n", NAMESPACE, "apply", "-f", str(ROOT / "k8s/spark/smoke-job.yaml")])
    wait_job("spark-smoke", "600s")
    logs = kubectl("logs", "job/spark-smoke", capture=True).stdout
    match = re.search(r"SMOKE_EVENTS=(\d+)", logs)
    if not match:
        raise CheckError(f"Spark smoke result missing from logs:\n{logs[-4000:]}")
    spark_events = int(match.group(1))

    trino_rows = trino_count()
    health = superset_health()
    nodes = node_status()
    if len(nodes) != 3 or not all(node["ready"] for node in nodes):
        raise CheckError(f"expected 3 Ready nodes, got {nodes}")
    buckets = read_buckets()
    if set(buckets) != {"raw", "bronze", "silver", "gold"}:
        raise CheckError(f"unexpected buckets: {buckets}")

    evidence = {
        "issue": "https://github.com/vincentlalo-long/mining-opensource-project/issues/2",
        "run_id": str(uuid.uuid4()),
        "recorded_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "cluster": CLUSTER,
        "namespace": NAMESPACE,
        "config": str(config),
        "smoke_hour": hour,
        "nodes": nodes,
        "buckets": buckets,
        "spark_events": spark_events,
        "trino_nation_rows": trino_rows,
        "superset_health": health,
        "status": "complete",
    }
    path = ROOT / "results/issue_2_smoke.json"
    path.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    print(f"Platform evidence: {path.relative_to(ROOT)}; run_id={evidence['run_id']}")
    return 0


def down(_config):
    if not cluster_exists():
        print(f"cluster {CLUSTER} does not exist")
        return 0
    run(["k3d", "cluster", "delete", CLUSTER])
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=ROOT / "config/dev.yml", type=Path)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("doctor", help="check docker, k3d, kubectl, ports, disk and memory")
    up_parser = subparsers.add_parser("up", help="create the cluster and run the smoke checks")
    up_parser.add_argument("--hour", default=DEFAULT_HOUR)
    subparsers.add_parser("down", help="delete the k3d cluster")
    args = parser.parse_args(argv)
    try:
        if args.command == "doctor":
            return 0 if doctor(args.config) else 1
        if args.command == "up":
            return up(args.config, args.hour)
        if args.command == "down":
            return down(args.config)
    except CheckError as error:
        print(f"FAIL: {error}")
        return 1
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
