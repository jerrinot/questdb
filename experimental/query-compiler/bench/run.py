#!/usr/bin/env python3
"""Fixed-query runner, retained from target/dependent-transfer and target/rtabench.

Times complete cursor consumption; perf covers the configured measured executions.
Use --warmups 0 to include first use. Exact commands remain in the output directory.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import statistics
import subprocess
import time

HERE = Path(__file__).resolve().parent
MODULE = HERE.parent
REPO = MODULE.parents[1]


def read(path):
    try:
        return Path(path).read_text().strip()
    except OSError as error:
        return {"unavailable": str(error)}


def capture(command):
    try:
        result = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        return {"returncode": result.returncode, "output": result.stdout}
    except OSError as error:
        return {"unavailable": str(error)}


def digest(path):
    h = hashlib.sha256()
    paths = sorted(p for p in path.rglob("*") if p.is_file()) if path.is_dir() else [path]
    for file in paths:
        if path.is_dir():
            h.update(str(file.relative_to(path)).encode() + b"\0")
        with file.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                h.update(chunk)
    return h.hexdigest()


def cpu_list(spec):
    result = set()
    for part in spec.split(","):
        bounds = [int(x) for x in part.split("-")]
        if len(bounds) > 2 or min(bounds) < 0 or bounds[-1] < bounds[0]:
            raise ValueError("Invalid CPU list: " + spec)
        result.update(range(bounds[0], bounds[-1] + 1))
    return sorted(result)


def select_cpus(count, spec):
    allowed = os.sched_getaffinity(0)
    if spec:
        selected = cpu_list(spec)
    else:
        cores = {}
        for cpu in sorted(allowed):
            topology = Path(f"/sys/devices/system/cpu/cpu{cpu}/topology")
            siblings = read(topology / "thread_siblings_list")
            if not isinstance(siblings, str):
                raise ValueError("Missing CPU sibling topology; pass --cpus explicitly")
            # Core IDs can repeat across clusters in one socket (e.g. Apple M1).
            key = tuple(cpu_list(siblings))
            cores.setdefault(key, cpu)
        selected = list(cores.values())[:count]
    if len(selected) != count or not set(selected) <= allowed:
        raise ValueError(f"Need {count} permitted CPUs; selected {selected}, allowed {sorted(allowed)}")
    return selected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["stock", "java", "llvm", "load"])
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--query", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--core", type=Path, help="Exact core classes/JAR; required for stock")
    parser.add_argument("--compiler", type=Path, default=MODULE / "target/classes")
    parser.add_argument("--native", type=Path, default=MODULE / "target/native")
    parser.add_argument("--cores", type=int, default=8)
    parser.add_argument("--cpus", help="CPU list; defaults to permitted physical cores")
    parser.add_argument("--warmups", type=int, default=10)
    parser.add_argument("--measured", type=int, default=5)
    parser.add_argument("--profile", choices=["time", "stat", "cpu"], default="stat")
    parser.add_argument("--events", default="{cycles:u,instructions:u,branch-misses:u},task-clock,context-switches,page-faults,major-faults")
    parser.add_argument("--system-wide", action="store_true", help="For discovered uncore counters; includes background work")
    parser.add_argument("--record-event", help="Discovered precise sampling event, used with --profile cpu")
    parser.add_argument("--java-join", action="store_true", help="Diagnostic: same generated plan, Java join updates")
    parser.add_argument("--no-group-cache", action="store_true", help="Diagnostic: suppress only destination-cache admission")
    parser.add_argument("--lookahead", choices=["NONE", "JOINS", "JOINS_AND_GROUPS"], help="Diagnostic: freeze existing join alternatives")
    args = parser.parse_args()
    if args.mode == "stock" and args.core is None:
        parser.error("stock requires --core pointing to an identified stock build")
    if args.mode != "llvm" and (args.java_join or args.no_group_cache or args.lookahead):
        parser.error("join diagnostics require llvm mode")
    if args.java_join and (args.no_group_cache or args.lookahead):
        parser.error("Java updates do not consume generated join alternatives")
    if args.record_event and args.profile != "cpu" or args.system_wide and args.profile != "stat":
        parser.error("--record-event requires cpu profiling; --system-wide requires stat")
    if args.cores < 1 or args.measured < 1 or args.warmups < 0:
        parser.error("invalid core/run count")
    if args.mode == "load":
        if args.db.exists():
            parser.error("load requires a new database root")
        args.profile, args.warmups, args.measured = "time", 0, 0
    cpus = select_cpus(args.cores, args.cpus)
    allowed = os.sched_getaffinity(0)
    # Keep the sampler and result handling off benchmark cores when possible.
    excluded = set(cpus)
    for cpu in cpus:
        siblings = read(f"/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list")
        if isinstance(siblings, str):
            excluded.update(cpu_list(siblings))
    observer = allowed - excluded
    if observer:
        os.sched_setaffinity(0, observer)
    core = (args.core or REPO / "core/target/classes").resolve()
    compiler, native = args.compiler.resolve(), args.native.resolve()
    db, query, out = args.db.resolve(), args.query.resolve(), args.out.resolve()
    libraries = [native / f"libquery_codegen_{name}.so" for name in ("llvm", "regex")]
    required = [core, compiler, query, *libraries]
    if args.mode != "load":
        required.append(db / "db")
    for path in required:
        if not path.exists():
            parser.error(f"Missing input: {path}")
    if not query.is_file():
        parser.error("Run one query at a time")
    out.mkdir(parents=True, exist_ok=False)
    if args.mode == "load":
        db.mkdir(parents=True, exist_ok=False)
    harness = out / "harness"
    harness.mkdir()
    # Compile against current interfaces even when stock is the runtime control.
    subprocess.run(["javac", "-cp", os.pathsep.join(map(str, [compiler, REPO / "core/target/classes"])),
                    "-d", str(harness), *map(str, HERE.glob("*.java"))], check=True)
    command = ["taskset", "-c", ",".join(map(str, cpus)), "java", "-Xmx4g",
               "-XX:+UnlockDiagnosticVMOptions", "-XX:+PreserveFramePointer", "-XX:+DumpPerfMapAtExit",
               "-Xlog:gc:file=" + str(out / "gc.log"), "-Dout=" + str(MODULE / "parallel-log.conf"),
               "-Dquestdb.codegen.llvm.library=" + str(libraries[0]),
               "-Dquestdb.codegen.regex.library=" + str(libraries[1]),
               "--enable-native-access=ALL-UNNAMED", "--sun-misc-unsafe-memory-access=allow",
               "--add-opens=java.base/java.lang=ALL-UNNAMED", "--add-opens=java.base/java.nio=ALL-UNNAMED",
               "--add-opens=java.base/java.time.zone=ALL-UNNAMED", "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED"]
    if args.java_join:
        command.append("-Dbench.javaJoin=true")
    if args.no_group_cache:
        command.append("-Dbench.noGroupCache=true")
    if args.lookahead:
        command.append("-Dbench.lookahead=" + args.lookahead)
    command += ["-cp", os.pathsep.join(map(str, [harness, compiler, core])), "RtaMain", args.mode,
                str(args.cores), str(db), str(query), str(out)]
    symbols = out / "symbols"
    symbols.mkdir()
    env = os.environ | {"RTA_REPS": str(args.warmups + args.measured), "RTA_WARMUPS": str(args.warmups),
                        "RTA_DROP_CACHE": "0", "QUERY_CODEGEN_PROFILE_DIR": str(symbols)}
    if args.profile != "time":
        for name in ("ctl", "ack"):
            os.mkfifo(out / name)
        env |= {"QUERY_CODEGEN_PERF_CONTROL": str(out / "ctl"), "QUERY_CODEGEN_PERF_ACK": str(out / "ack")}
        control = ["--delay=-1", f"--control=fifo:{out / 'ctl'},{out / 'ack'}"]
        if args.profile == "stat":
            prefix = ["perf", "stat", *control, "-x", ";", "-o", str(out / "perf.csv"), "-e", args.events]
            if args.system_wide:
                prefix.append("-a")
        else:
            sampling = ["-e", args.record_event, "-d", "-W", "-c", "1048576"] if args.record_event else ["-F", "199", "--call-graph", "fp"]
            prefix = ["perf", "record", *control, *sampling, "-o", str(out / "perf.data")]
        command = prefix + ["--"] + command
    identity = {"head": capture(["git", "-C", str(REPO), "rev-parse", "HEAD"]),
                "status": capture(["git", "-C", str(REPO), "status", "--short"]),
                "sha256": {str(p): digest(p) for p in [core, compiler, query, *libraries, *HERE.glob("*.java"), Path(__file__)]},
                "allowed_cpus": sorted(allowed), "selected_cpus": cpus,
                "observer_cpus": sorted(os.sched_getaffinity(0)),
                "db": str(db), "options": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
                "environment": {key: value for key, value in env.items() if key.startswith(("RTA_", "QUERY_CODEGEN_"))}}
    if (REPO / "SOURCE_REVISION").exists():
        identity["archive_revision"] = read(REPO / "SOURCE_REVISION")
    for name, cmd in {"cpu": ["lscpu"], "numa": ["numactl", "--hardware"], "java": ["java", "-version"],
                      "perf": ["perf", "--version"], "kernel": ["uname", "-a"],
                      "native_dependencies": ["ldd", str(libraries[0])],
                      "filesystem": ["findmnt", "-T", str(db)], "space": ["df", "-B1", str(db)],
                      "processes": ["ps", "-eo", "pid,pcpu,pmem,comm", "--sort=-pcpu"]}.items():
        identity[name] = capture(cmd)
    identity["cpu_sysfs"] = {str(cpu): {str(p): read(p) for pattern in ("topology/*", "cache/index*/*", "cpufreq/*")
                                       for p in Path(f"/sys/devices/system/cpu/cpu{cpu}").glob(pattern) if p.is_file()} for cpu in cpus}
    identity["limits"] = {p: read(p) for p in ("/proc/self/limits", "/proc/self/cgroup", "/proc/sys/kernel/perf_event_paranoid", "/proc/sys/vm/max_map_count")}
    identity["memory_policy"] = {p: read(p) for p in ("/sys/kernel/mm/transparent_hugepage/enabled", "/proc/sys/kernel/numa_balancing")}
    (out / "identity.json").write_text(json.dumps(identity, indent=2))
    (out / "command.json").write_text(json.dumps(command, indent=2))
    pid = None
    with (out / "run.log").open("w") as log, (out / "use.jsonl").open("w") as evidence:
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, env=env)
        try:
            while process.poll() is None:
                log_state = (out / "run.log").read_text()
                match = re.search(r"^PID=(\d+)", log_state, re.M)
                if match:
                    pid = int(match[1])
                paths = ["/proc/stat", "/proc/meminfo", "/proc/vmstat", "/proc/diskstats", "/proc/net/dev",
                         "/proc/pressure/cpu", "/proc/pressure/memory", "/proc/pressure/io"]
                if pid:
                    # numa_maps walks page tables under mmap_lock and can stall
                    # query allocations. Capture placement separately from timed runs.
                    paths += [f"/proc/{pid}/{name}" for name in ("io", "stat", "status", "limits")]
                    cg = read(f"/proc/{pid}/cgroup")
                    if isinstance(cg, str) and cg.startswith("0::"):
                        group = Path("/sys/fs/cgroup") / cg[3:].lstrip("/")
                        while group.is_relative_to("/sys/fs/cgroup"):
                            paths += [str(group / name) for name in ("cpu.max", "cpu.stat", "memory.max", "memory.events", "io.max")]
                            group = group.parent
                sample = {"wall_time": time.time(), "time": time.monotonic(), "profile_enabled": log_state.rfind("Events enabled") > log_state.rfind("Events disabled"),
                          "files": {p: read(p) for p in paths},
                          "freq_khz": {str(cpu): read(f"/sys/devices/system/cpu/cpu{cpu}/cpufreq/scaling_cur_freq") for cpu in cpus}}
                if pid:
                    sample["scheduler"] = {p.parent.name: read(p) for p in Path(f"/proc/{pid}/task").glob("*/schedstat")}
                evidence.write(json.dumps(sample) + "\n")
                evidence.flush()
                time.sleep(.2)
        finally:
            if process.poll() is None:
                process.terminate()
            process.wait()
    (out / "exit.json").write_text(json.dumps({"returncode": process.returncode}))
    if pid:
        perfmap = Path(f"/tmp/perf-{pid}.map")
        with perfmap.open("a") as combined:
            for path in symbols.glob("*-native.map"):
                combined.write(path.read_text())
        (symbols / "combined-perf.map").write_text(perfmap.read_text())
    log = (out / "run.log").read_text()
    samples = [int(x) / 1e6 for x in re.findall(r"RESULT .*? ns=(\d+)", log)]
    summary = {"runs_ms": samples, "measured_median_ms": statistics.median(samples[args.warmups:]) if len(samples) > args.warmups else None}
    (out / "summary.json").write_text(json.dumps(summary, indent=2))
    print(out.name, json.dumps(summary), flush=True)
    if process.returncode or list(out.glob("*/error.txt")) or len(samples) != args.warmups + args.measured:
        raise SystemExit("Failed query; inspect " + str(out / "run.log"))


if __name__ == "__main__":
    main()
