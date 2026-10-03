"""Keep one persistent local database under Amp's service supervisor."""

import signal
import subprocess
import sys

CLUSTER_FILE = "/etc/foundationdb/fdb.cluster"


def stop(signum, frame):
    raise SystemExit(0)


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)

# This child belongs to the supervised service, not to setup or a test process.
server = subprocess.Popen([
    "/usr/sbin/fdbserver",
    "--cluster-file", CLUSTER_FILE,
    "--public-address", "127.0.0.1:4500",
    "--listen-address", "127.0.0.1:4500",
    "--datadir", "/var/lib/foundationdb/data/4500",
    "--logdir", "/var/log/foundationdb",
    "--logsize", "10MiB",
    "--maxlogssize", "100MiB",
])
try:
    # 'new' refuses to change an existing database. On subsequent starts it
    # reports that the database exists; only the transactional read must succeed.
    subprocess.run([
        "/usr/bin/fdbcli", "-C", CLUSTER_FILE, "--timeout", "30",
        "--exec", "configure new single ssd",
    ], check=False, timeout=35)
    subprocess.run([
        "/usr/bin/fdbcli", "-C", CLUSTER_FILE, "--timeout", "30",
        "--exec", "get __amp_foundationdb_readiness__",
    ], check=True, timeout=35)
    print("FoundationDB ready: " + CLUSTER_FILE, flush=True)
    sys.exit(server.wait())
finally:
    server.terminate()
    try:
        server.wait(timeout=15)
    except subprocess.TimeoutExpired:
        server.kill()
        server.wait()
