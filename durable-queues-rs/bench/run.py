#!/usr/bin/env python3
"""Phase 7 benchmark driver: Java Essentials 0.60.0 vs the Rust port, as containers under equal limits.

For each language it measures, against one dedicated PostgreSQL:
  startup   time from `docker run` to the first message handled (a backlog is waiting), repeated
  idle      memory and CPU of a consumer with nothing to do
  rate L    open-loop load at L msg/s: throughput, enqueue->handle latency, CPU, memory, DB connections and
            pg_stat_statements, each level after a warm-up
  saturation  a backlog of N messages drained by a fresh consumer: messages per second

The workload generator is the same for both languages (`interop bench-produce`, Rust). Only the consumer differs.
Standard library only. Run from anywhere:  python3 durable-queues-rs/bench/run.py [--quick]
"""

import argparse
import gzip
import json
import statistics
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

BENCH = Path(__file__).resolve().parent
CRATE = BENCH.parent
WORKSPACE = CRATE.parent

NETWORK = "dq-bench"
PG = "dq-bench-pg"
PG_IMAGE = "postgres:17-alpine"
JAVA_IMAGE = "durable-queues-java-harness:interop"
RUST_IMAGE = "durable-queues-rust-node:bench"
DB = "postgres"

# Held constant across languages (IMPLEMENTATION_PLAN.md Phase 7)
NODE_LIMITS = ["--cpus", "2", "--memory", "1g"]
GENERATOR_LIMITS = ["--cpus", "2", "--memory", "512m"]
PG_LIMITS = ["--cpus", "2", "--memory", "2g"]
POOL_SIZE = 10

# What is compared. `rust-fixed-rate` polls like Java's centralized fetcher (one poll per 20 ms) and is the
# language-to-language comparison; `rust-eager` is the crate's default loop, an algorithm difference reported apart.
VARIANTS = {
    "java": ("java", []),
    "rust-fixed-rate": ("rust", ["--fetch-pacing", "fixed-rate"]),
    "rust-eager": ("rust", ["--fetch-pacing", "eager"]),
}
QUEUE = "Bench"


def sh(*args, check=True) -> str:
    result = subprocess.run(args, check=False, text=True, capture_output=True)
    if check and result.returncode != 0:
        raise RuntimeError(f"{' '.join(args)} failed ({result.returncode}): {result.stderr}")
    return result.stdout


def psql(sql):
    return sh("docker", "exec", PG, "psql", "-U", "postgres", "-d", DB, "-At", "-c", sql).strip()


def log(message):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def build_images():
    log("building images")
    sh("docker", "build", "-q", "-t", JAVA_IMAGE, str(CRATE / "interop/java-harness"))
    sh("docker", "build", "-q", "-f", str(CRATE / "interop/rust-node/Dockerfile"), "-t", RUST_IMAGE, str(WORKSPACE))


def start_postgres(auto_explain_ms=None):
    stop_all()
    sh("docker", "network", "create", NETWORK)
    settings = ["-c", "shared_preload_libraries=pg_stat_statements", "-c", "max_connections=200"]
    if auto_explain_ms is not None:
        # Diagnostics only: logs the plan of every statement slower than this (no ANALYZE, so little overhead)
        settings = ["-c", "shared_preload_libraries=pg_stat_statements,auto_explain",
                    "-c", f"auto_explain.log_min_duration={auto_explain_ms}", "-c", "max_connections=200"]
    sh("docker", "run", "-d", "--name", PG, "--network", NETWORK, *PG_LIMITS,
       "-e", "POSTGRES_PASSWORD=postgres", PG_IMAGE, *settings)
    for _ in range(100):
        # Over TCP: during initdb the image runs a temporary server that listens on the Unix socket only
        ready = ["docker", "exec", PG, "pg_isready", "-h", "127.0.0.1", "-U", "postgres"]
        if subprocess.run(ready, capture_output=True).returncode == 0:
            try:
                psql("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
                return
            except RuntimeError:
                pass
        time.sleep(0.2)
    raise RuntimeError("PostgreSQL did not start")


def stop_all():
    names = sh("docker", "ps", "-aq", "--filter", "name=dq-bench").split()
    if names:
        sh("docker", "rm", "-f", "-v", *names)
    sh("docker", "network", "rm", NETWORK, check=False)


def node_args(lang):
    if lang == "java":
        return [JAVA_IMAGE, "--jdbc-url", f"jdbc:postgresql://{PG}:5432/{DB}"]
    return [RUST_IMAGE, "--database-url", f"postgres://postgres:postgres@{PG}:5432/{DB}"]


def run_node(lang, name, command, limits, *options):
    image, *connection = node_args(lang)
    sh("docker", "run", "-d", "--name", name, "--network", NETWORK, *limits, image, command,
       "--queue", QUEUE, "--pool-size", str(POOL_SIZE), "--node", NODE_NAMES.get(command, lang), *options,
       *connection)


# The `application_name` each node connects with, so connections are counted per node
NODE_NAMES = {"bench-produce": "generator"}


def generate(*options):
    """Runs the workload generator to completion; returns its `done` event."""
    name = "dq-bench-gen"
    run_node("rust", name, "bench-produce", GENERATOR_LIMITS, *options)
    exit_code = sh("docker", "wait", name).strip()
    logs = subprocess.run(["docker", "logs", name], capture_output=True, text=True)
    sh("docker", "rm", "-v", name)
    events = [json.loads(line) for line in logs.stdout.splitlines() if line.startswith("{")]
    done = [e for e in events if e["event"] == "done"]
    if not done:
        raise RuntimeError(f"workload generator exited with {exit_code}: {logs.stderr[-2000:]}")
    return done[0]


def reset_queue():
    if table_exists():
        psql(f"DELETE FROM durable_queues WHERE queue_name = '{QUEUE}'")


def table_exists():
    return psql("SELECT to_regclass('durable_queues') IS NOT NULL") == "t"


def queued():
    return int(psql(f"SELECT count(*) FROM durable_queues WHERE queue_name = '{QUEUE}'")) if table_exists() else 0


class Collector:
    """Follows a container's JSON events, its `docker stats` and the database's connections, timestamped."""

    def __init__(self, container, application_name):
        self.container = container
        self.application_name = application_name
        self.events, self.stats, self.connections = [], [], []
        self.lock = threading.Lock()
        self.running = True
        self.procs = []
        self._follow(["docker", "logs", "-f", container], self._on_event)
        self._follow(["docker", "stats", "--format", "{{json .}}", container], self._on_stats)
        threading.Thread(target=self._poll_connections, daemon=True).start()

    def _follow(self, cmd, on_line):
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        self.procs.append(proc)
        stdout = proc.stdout
        assert stdout is not None

        def read():
            for line in stdout:
                on_line(line.strip())

        threading.Thread(target=read, daemon=True).start()

    def _on_event(self, line):
        if line.startswith("{"):
            with self.lock:
                self.events.append((time.time(), json.loads(line)))

    def _on_stats(self, line):
        # `docker stats` redraws with ANSI escapes; the JSON object is the part between the braces
        start, end = line.find("{"), line.rfind("}")
        if start < 0:
            return
        sample = json.loads(line[start:end + 1])
        cpu = float(sample["CPUPerc"].rstrip("%") or 0)
        mem = parse_bytes(sample["MemUsage"].split("/")[0])
        with self.lock:
            self.stats.append((time.time(), cpu, mem))

    def _poll_connections(self):
        while self.running:
            try:
                rows = psql("SELECT coalesce(application_name, ''), count(*) FROM pg_stat_activity "
                            "WHERE backend_type = 'client backend' AND datname = current_database() "
                            "AND pid <> pg_backend_pid() GROUP BY 1")
                counts = dict(row.split("|") for row in rows.splitlines() if row)
                with self.lock:
                    self.connections.append((time.time(), {k: int(v) for k, v in counts.items()}))
            except RuntimeError:
                pass
            time.sleep(1)

    def wait_for(self, kind, timeout=120):
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self.lock:
                found = [e for t, e in self.events if e["event"] == kind]
            if found:
                return found[0]
            time.sleep(0.05)
        raise RuntimeError(f"{self.container}: no '{kind}' event within {timeout}s")

    def window(self, start, end):
        with self.lock:
            ticks = [e for t, e in self.events if e["event"] == "tick" and start <= e["at"] / 1000 < end]
            stats = [(c, m) for t, c, m in self.stats if start <= t < end]
            conns = [c for t, c in self.connections if start <= t < end]
        return ticks, stats, conns

    def stop(self):
        self.running = False
        for proc in self.procs:
            proc.terminate()


def parse_bytes(text):
    text = text.strip()
    units = {"B": 1, "KiB": 1024, "MiB": 1024 ** 2, "GiB": 1024 ** 3, "kB": 1000, "MB": 1000 ** 2, "GB": 1000 ** 3}
    for unit in sorted(units, key=len, reverse=True):
        if text.endswith(unit):
            return float(text[: -len(unit)]) * units[unit]
    return float(text)


def percentile(sorted_values, p) -> float:
    return sorted_values[min(len(sorted_values) - 1, int(round(p / 100 * (len(sorted_values) - 1))))]


def summarize(ticks, stats, conns, seconds, application_name):
    latencies = sorted(lat for tick in ticks for lat in tick["latencies_us"])
    handled = sum(tick["count"] for tick in ticks)
    cpu = [c for c, _ in stats]
    mem = [m for _, m in stats]
    node_conns = [c.get(application_name, 0) for c in conns]
    return {
        "handled": handled,
        "throughput_per_s": round(handled / seconds, 1) if seconds else None,
        "latency_ms": {name: (round(percentile(latencies, p) / 1000, 2) if latencies else None)
                       for name, p in (("p50", 50), ("p90", 90), ("p99", 99), ("max", 100))},
        "cpu_percent": {"avg": round(statistics.mean(cpu), 1) if cpu else None, "max": max(cpu, default=None)},
        "memory_mib": {"avg": round(statistics.mean(mem) / 2 ** 20, 1) if mem else None,
                       "max": round(max(mem) / 2 ** 20, 1) if mem else None},
        "db_connections": {"avg": round(statistics.mean(node_conns), 1) if node_conns else None,
                           "max": max(node_conns, default=None)},
    }


def top_statements():
    rows = psql("SELECT calls, round(total_exec_time::numeric, 1), rows, "
                "regexp_replace(left(query, 160), '\\s+', ' ', 'g') FROM pg_stat_statements "
                "WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) "
                "ORDER BY total_exec_time DESC LIMIT 12")
    result = []
    for row in rows.splitlines():
        calls, total_ms, nrows, query = row.split("|", 3)
        result.append({"calls": int(calls), "total_ms": float(total_ms), "rows": int(nrows), "query": query})
    return result


def write_raw(out, name, collector):
    with gzip.open(out / "raw" / f"{name}.jsonl.gz", "wt") as f:
        with collector.lock:
            for t, event in collector.events:
                f.write(json.dumps({"kind": "event", "t": t, **event}) + "\n")
            for t, cpu, mem in collector.stats:
                f.write(json.dumps({"kind": "stats", "t": t, "cpu_percent": cpu, "mem_bytes": mem}) + "\n")
            for t, conns in collector.connections:
                f.write(json.dumps({"kind": "connections", "t": t, "by_application": conns}) + "\n")


def measure_startup(variant, consume, reps):
    results = []
    for rep in range(reps):
        reset_queue()
        generate("--count", "1000")
        name = f"dq-bench-{variant}-startup"
        started = time.time()
        consume(name)
        collector = Collector(name, None)
        first = collector.wait_for("first")
        results.append(round(first["at"] - started * 1000))
        collector.stop()
        sh("docker", "rm", "-f", "-v", name)
        log(f"  startup {rep + 1}/{reps}: {results[-1]} ms")
    return {"runs_ms": results, "median_ms": statistics.median(results)}


def measure(variant, parallel, args, out):
    lang, pacing = VARIANTS[variant]
    label = f"{variant} p{parallel}"

    def consume(name):
        run_node(lang, name, "bench-consume", NODE_LIMITS, "--parallel", str(parallel), *pacing)

    log(f"{label}: startup")
    result = {"startup": measure_startup(variant, consume, args.startup_reps)}
    pin_statistics(args.saturation_count)

    name = f"dq-bench-{variant}"
    consume(name)
    collector = Collector(name, lang)
    collector.wait_for("ready")
    log(f"{label}: idle")
    time.sleep(args.settle_s)
    start = time.time()
    time.sleep(args.idle_s)
    ticks, stats, conns = collector.window(start, time.time())
    result["idle"] = summarize(ticks, stats, conns, args.idle_s, lang)

    result["levels"] = {}
    for rate in args.levels:
        log(f"{label}: {rate} msg/s")
        pin_statistics(args.saturation_count)
        psql("SELECT pg_stat_statements_reset()")
        start = time.time()
        done = generate("--rate", str(rate), "--duration-s", str(args.warmup_s + args.duration_s))
        window = (start + args.warmup_s, start + args.warmup_s + args.duration_s)
        drained = wait_drained(args.drain_timeout_s)
        # A consumer that fell behind would take minutes to catch up; its backlog is recorded and purged instead
        backlog = queued()
        if backlog:
            reset_queue()
        ticks, stats, conns = collector.window(*window)
        level = summarize(ticks, stats, conns, args.duration_s, lang)
        level.update(offered_per_s=rate, generator_sent=done["sent"], drained_after_s=drained,
                     backlog_purged=backlog, statements=top_statements())
        result["levels"][str(rate)] = level
        log(f"  handled {level['throughput_per_s']}/s, p50 {level['latency_ms']['p50']} ms, "
            f"p99 {level['latency_ms']['p99']} ms, cpu {level['cpu_percent']['avg']}%")
    collector.stop()
    write_raw(out, f"{variant}-p{parallel}-steady", collector)
    sh("docker", "rm", "-f", "-v", name)

    for kind in ("saturation", "saturation_stale_stats"):
        result[kind] = drain_backlog(kind, variant, parallel, consume, lang, label, args, out)
    return result


def pin_statistics(rows):
    """Deterministic table state before every level and saturation run, the same for every variant.

    Autovacuum is off for the table for the whole run: a VACUUM resets the planner's row estimate to what is left
    (none, between levels), and statistics from an empty table make the claim degrade with the backlog (README,
    "Finding"). Instead: a manual VACUUM, which keeps bloat bounded, then statistics taken with `rows` queued."""
    reset_queue()
    psql("ALTER TABLE durable_queues SET (autovacuum_enabled = false)")
    psql("VACUUM durable_queues")
    generate("--count", str(rows))
    psql("ANALYZE durable_queues")
    reset_queue()


def drain_backlog(kind, variant, parallel, consume, lang, label, args, out):
    """A fresh consumer drains a backlog. `saturation` runs with pinned representative statistics;
    `saturation_stale_stats` with those of the empty table, so the planner misjudges the claim's join back to the
    table (README, "Finding")."""
    log(f"{label}: {kind} ({args.saturation_count} queued)")
    pin_statistics(args.saturation_count)
    stale = kind == "saturation_stale_stats"
    if stale:
        # Statistics of the emptied table: pages still allocated, rows estimated at 0 - what an analyze after a burst
        # leaves. (A VACUUM that truncates the table to 0 pages does not trigger it: the planner then falls back
        # to a size heuristic.)
        psql("ANALYZE durable_queues")
    generate("--count", str(args.saturation_count))
    psql("SELECT pg_stat_statements_reset()")
    name = f"dq-bench-{variant}-{kind.replace('_', '-')}"
    consume(name)
    collector = Collector(name, lang)
    first = collector.wait_for("first")["at"] / 1000
    empty_at = None
    while empty_at is None:
        if queued() == 0:
            empty_at = time.time()
        time.sleep(0.1)
    ticks, stats, conns = collector.window(first, empty_at + 1)
    drained = summarize(ticks, stats, conns, empty_at - first, lang)
    drained.update(queued=args.saturation_count, drain_s=round(empty_at - first, 2),
                   throughput_per_s=round(args.saturation_count / (empty_at - first), 1),
                   statements=top_statements())
    log(f"  drained at {drained['throughput_per_s']}/s")
    collector.stop()
    write_raw(out, f"{variant}-p{parallel}-{kind}", collector)
    sh("docker", "rm", "-f", "-v", name)
    return drained


def wait_drained(timeout):
    started = time.time()
    while time.time() - started < timeout:
        if queued() == 0:
            return round(time.time() - started, 1)
        time.sleep(0.2)
    return None


def sizes():
    def image_mib(image):
        return round(int(sh("docker", "image", "inspect", "-f", "{{.Size}}", image)) / 2 ** 20, 1)

    def file_mib(image, path):
        out = sh("docker", "run", "--rm", "--entrypoint", "stat", image, "-c", "%s", path)
        return round(int(out) / 2 ** 20, 1)

    return {
        "java": {"image_mib": image_mib(JAVA_IMAGE), "artifact_mib": file_mib(JAVA_IMAGE, "/app/java-harness.jar"),
                 "artifact": "fat jar (needs a JRE)"},
        "rust": {"image_mib": image_mib(RUST_IMAGE), "artifact_mib": file_mib(RUST_IMAGE, "/usr/local/bin/interop"),
                 "artifact": "release binary"},
    }


def consumer_sql(statements, seconds):
    """Calls and execution time per second of the consumer's own statements: claims, acks, retries, resets. The
    generator's INSERTs and the driver's polling are left out."""
    def is_consumer(query):
        q = " ".join(query.split())
        return "WITH cte_" in q or q.startswith("DELETE FROM durable_queues WHERE id") or q.startswith(
            "UPDATE durable_queues")

    own = [st for st in statements if is_consumer(st["query"])]
    return (round(sum(st["calls"] for st in own) / seconds, 1),
            round(sum(st["total_ms"] for st in own) / seconds, 1))


def claim_ms_per_call(statements):
    claims = [st for st in statements if "cte_unordered" in st["query"]]
    calls = sum(st["calls"] for st in claims)
    return round(sum(st["total_ms"] for st in claims) / calls, 2) if calls else None


def write_report(results, out):
    def fmt(value, unit=""):
        return "-" if value is None else f"{value}{unit}"

    lines = [f"# Benchmark results {results['started']}", "",
             "Generated by `bench/run.py`; raw data in `raw/`, everything below in `summary.json`.", ""]
    settings = results["settings"]
    lines += ["## Settings", "", "```json", json.dumps(settings, indent=2), "```", ""]
    sizes = results["sizes"]
    lines += ["## Sizes", "", "| | Java | Rust |", "|---|---|---|",
              f"| Container image | {sizes['java']['image_mib']} MiB | {sizes['rust']['image_mib']} MiB |",
              f"| Artifact | {sizes['java']['artifact_mib']} MiB ({sizes['java']['artifact']}) | "
              f"{sizes['rust']['artifact_mib']} MiB ({sizes['rust']['artifact']}) |", ""]
    for parallel in settings["parallel_consumers"]:
        runs = results.get(f"p{parallel}", {})
        variants = list(runs)
        if not variants:
            continue
        lines += [f"## parallel_consumers = {parallel}", "",
                  "| | " + " | ".join(variants) + " |", "|---" * (len(variants) + 1) + "|"]

        def row(name, cell):
            lines.append(f"| {name} | " + " | ".join(cell(runs[v]) for v in variants) + " |")

        row("Startup to first message (median)", lambda r: fmt(r["startup"]["median_ms"], " ms"))
        row("Idle memory", lambda r: fmt(r["idle"]["memory_mib"]["avg"], " MiB"))
        row("Idle CPU", lambda r: fmt(r["idle"]["cpu_percent"]["avg"], " %"))
        row("Idle DB connections", lambda r: fmt(r["idle"]["db_connections"]["avg"]))
        for rate in settings["levels"]:
            key = str(rate)
            row(f"**{rate} msg/s** handled", lambda r: fmt(r["levels"][key]["throughput_per_s"], "/s"))
            row(f"{rate}: latency p50 / p99", lambda r: f"{fmt(r['levels'][key]['latency_ms']['p50'])} / "
                                                        f"{fmt(r['levels'][key]['latency_ms']['p99'], ' ms')}")
            row(f"{rate}: CPU avg", lambda r: fmt(r["levels"][key]["cpu_percent"]["avg"], " %"))
            row(f"{rate}: memory avg / max", lambda r: f"{fmt(r['levels'][key]['memory_mib']['avg'])} / "
                                                         f"{fmt(r['levels'][key]['memory_mib']['max'], ' MiB')}")
            row(f"{rate}: DB connections avg / max", lambda r: f"{fmt(r['levels'][key]['db_connections']['avg'])} / "
                                                                 f"{fmt(r['levels'][key]['db_connections']['max'])}")
            row(f"{rate}: consumer SQL calls/s, exec ms/s",
                lambda r: "{} , {}".format(*consumer_sql(r["levels"][key]["statements"], settings["duration_s"])))
            row(f"{rate}: backlog purged after the level", lambda r: fmt(r["levels"][key]["backlog_purged"]))
        row(f"**Saturation** ({settings['saturation_count']} queued) drain rate",
            lambda r: fmt(r["saturation"]["throughput_per_s"], "/s"))
        row("Saturation: CPU avg / memory max", lambda r: f"{fmt(r['saturation']['cpu_percent']['avg'])} % / "
                                                        f"{fmt(r['saturation']['memory_mib']['max'], ' MiB')}")
        row("Saturation: consumer SQL calls/s",
            lambda r: fmt(consumer_sql(r["saturation"]["statements"], r["saturation"]["drain_s"])[0]))
        row("**Saturation, stale statistics**: drain rate",
            lambda r: fmt(r["saturation_stale_stats"]["throughput_per_s"], "/s"))
        row("Saturation, stale statistics: claim ms per call",
            lambda r: fmt(claim_ms_per_call(r["saturation_stale_stats"]["statements"])))
        lines.append("")
    (out / "results.md").write_text("\n".join(lines))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--variants", default=",".join(VARIANTS))
    parser.add_argument("--parallel", default="8,128", help="parallel_consumers settings, each a full run")
    parser.add_argument("--levels", default="100,1000,5000")
    parser.add_argument("--warmup-s", type=int, default=10)
    parser.add_argument("--duration-s", type=int, default=30)
    parser.add_argument("--idle-s", type=int, default=15)
    parser.add_argument("--settle-s", type=int, default=10)
    parser.add_argument("--drain-timeout-s", type=int, default=15)
    parser.add_argument("--saturation-count", type=int, default=20000)
    parser.add_argument("--startup-reps", type=int, default=3)
    parser.add_argument("--quick", action="store_true", help="short smoke run of the whole procedure")
    parser.add_argument("--out", type=Path)
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--auto-explain-ms", type=int, help="diagnostics: log plans of statements slower than this")
    args = parser.parse_args()
    if args.quick:
        args.warmup_s, args.duration_s, args.idle_s, args.settle_s = 2, 5, 3, 2
        args.saturation_count, args.startup_reps, args.levels = 2000, 1, "100,1000"
    args.levels = [int(level) for level in args.levels.split(",")]
    args.parallel = [int(p) for p in args.parallel.split(",")]
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = args.out or BENCH / "results" / (stamp + ("-quick" if args.quick else ""))
    (out / "raw").mkdir(parents=True, exist_ok=True)

    if not args.no_build:
        build_images()
    results = {
        "started": stamp,
        "settings": {"node_limits": NODE_LIMITS, "generator_limits": GENERATOR_LIMITS, "postgres_limits": PG_LIMITS,
                     "postgres_image": PG_IMAGE, "parallel_consumers": args.parallel, "pool_size": POOL_SIZE,
                     "polling_interval_ms": 20, "queues": 1, "handler": "no-op",
                     "warmup_s": args.warmup_s, "duration_s": args.duration_s, "levels": args.levels,
                     "saturation_count": args.saturation_count,
                     "host": {"cpus": sh("nproc").strip(),
                              "docker_mem_bytes": sh("docker", "info", "-f", "{{.MemTotal}}").strip(),
                              "arch": sh("uname", "-m").strip()}},
        "sizes": sizes(),
    }
    try:
        for parallel in args.parallel:
            results[f"p{parallel}"] = {}
            for variant in args.variants.split(","):
                # A fresh database per run, so none inherits another's table bloat or statistics
                start_postgres(args.auto_explain_ms)
                try:
                    results[f"p{parallel}"][variant] = measure(variant, parallel, args, out)
                finally:
                    if args.auto_explain_ms is not None:
                        (out / "raw" / f"postgres-{variant}-p{parallel}.log").write_text(
                            subprocess.run(["docker", "logs", PG], capture_output=True, text=True).stderr)
    finally:
        stop_all()
    (out / "summary.json").write_text(json.dumps(results, indent=2) + "\n")
    write_report(results, out)
    log(f"results in {out}")


if __name__ == "__main__":
    main()
