"""Run the consume benchmark across four clients and print the comparison.

    pixi run broker-up
    pixi run -e interop bench
    pixi run broker-down

Seeds a fresh topic, then drains it with every client and mode available on
this machine. Local only, like the Docker suites; CI runs none of it.

Four peers, and the point of having four is that each one bounds a different
claim:

    c          librdkafka driven directly -- the ceiling. Nothing that
               calls this library can beat it, so it is what "close to
               native" is measured against.
    rust       rust-rdkafka. A zero-copy binding in a systems language,
               against the same librdkafka. The peer that makes the
               borrowed-view claim falsifiable rather than merely flattering.
    mojo       this package.
    confluent  confluent-kafka. The Python client this package's API follows.

**Three of the four load the same librdkafka; `confluent-kafka` does not.**
Its manylinux wheel bundles its own build (`librdkafka-c87086af.so.1`), so
while the version string matches, the compiler and flags need not. Treat the
mojo/c/rust rows as binding-against-binding and the confluent row as
client-against-client.

Methodology, in the order the mistakes were made:

- **Repeats happen inside each peer process**, by seeking back to offset 0.
  One drain per process left a spread wide enough to swamp every effect worth
  measuring.
- **A repeat that stalled is discarded, not averaged.** Each peer drains with
  a zero timeout and counts empty returns; with the topic already in the
  local queue there should be none, so a non-zero count means that repeat
  went to the network and is measuring the broker. This is a detector, and it
  is why the old `--prefetch-ms` guesswork is gone from the analysis even
  though the pause remains.
- **The plan is interleaved across rounds**, so a machine that drifts slower
  over time penalises every configuration equally instead of whichever ran
  last.
- **The median of clean repeats is reported**, with the spread beside it.
  Best-of-N rewards whichever client got luckiest.
- **Every peer sums the first payload byte of every record** and prints the
  checksum. `run.py` asserts every client agrees, so a client cannot look
  fast by skipping work.
"""

import argparse
import os
import re
import shutil
import statistics
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BENCH = os.path.join(ROOT, "benchmarks")
BUILD = os.path.join(BENCH, ".build")
RESULT = re.compile(
    r"^RESULT (\S+) (\S+) (\d+) (\d+) (\d+) (\d+) (\d+)$", re.M
)


def sh(cmd, env=None, timeout=3600):
    proc = subprocess.run(
        cmd, cwd=ROOT, capture_output=True, text=True, timeout=timeout,
        env=env,
    )
    return proc


def must(cmd, env=None):
    proc = sh(cmd, env)
    if proc.returncode != 0:
        sys.stderr.write(proc.stdout + proc.stderr)
        raise SystemExit(f"command failed: {' '.join(cmd)}")
    return proc.stdout


def prefix() -> str:
    return os.environ.get("CONDA_PREFIX", "")


def peer_env() -> dict:
    env = dict(os.environ)
    lib = os.path.join(prefix(), "lib")
    env["LD_LIBRARY_PATH"] = lib + os.pathsep + env.get("LD_LIBRARY_PATH", "")
    return env


def build_mojo() -> list[str] | None:
    """AOT-build the Mojo peer. `mojo run` would JIT it on every invocation."""
    out = os.path.join(BUILD, "bench_consume_mojo")
    proc = sh(["mojo", "build", "-I", "src",
               "benchmarks/bench_consume.mojo", "-o", out])
    if proc.returncode != 0:
        sys.stderr.write(proc.stdout + proc.stderr)
        return None
    return [out]


def build_c() -> list[str] | None:
    """The native ceiling. Skipped rather than fatal if there is no cc."""
    cc = shutil.which("cc") or shutil.which("gcc")
    if not cc or not prefix():
        print("  (skipping c peer: no compiler or no CONDA_PREFIX)")
        return None
    out = os.path.join(BUILD, "bench_consume_c")
    proc = sh([cc, "-O2", "benchmarks/bench_consume.c", "-o", out,
               "-I", os.path.join(prefix(), "include"),
               "-L", os.path.join(prefix(), "lib"), "-lrdkafka",
               "-Wl,-rpath," + os.path.join(prefix(), "lib")])
    if proc.returncode != 0:
        sys.stderr.write(proc.stdout + proc.stderr)
        print("  (skipping c peer: it did not compile)")
        return None
    return [out]


def build_rust() -> list[str] | None:
    """rust-rdkafka, linked against the same librdkafka via pkg-config."""
    if not shutil.which("cargo"):
        print("  (skipping rust peer: cargo is not on PATH)")
        return None
    env = dict(os.environ)
    env["PKG_CONFIG_PATH"] = os.path.join(prefix(), "lib", "pkgconfig")
    proc = sh(["cargo", "build", "--release",
               "--manifest-path", "benchmarks/rust_peer/Cargo.toml"],
              env=env, timeout=1800)
    if proc.returncode != 0:
        sys.stderr.write(proc.stdout + proc.stderr)
        print("  (skipping rust peer: it did not build)")
        return None
    return [os.path.join(
        ROOT, "benchmarks/rust_peer/target/release/bench_consume_rs")]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=os.environ.get(
        "MOJO_KAFKA_BOOTSTRAP", "localhost:9092"))
    ap.add_argument("--count", type=int, default=200_000)
    ap.add_argument("--value-bytes", type=int, default=100)
    ap.add_argument("--batch", type=int, default=1000)
    ap.add_argument("--repeat", type=int, default=3,
                    help="timed drains inside each peer process")
    ap.add_argument("--rounds", type=int, default=2,
                    help="times the whole plan is run, to interleave drift")
    ap.add_argument("--prefetch-ms", type=int, default=2500)
    ap.add_argument("--pin", default="",
                    help="taskset cpu list, e.g. 5,6,7")
    args = ap.parse_args()

    os.makedirs(BUILD, exist_ok=True)
    print("building peers ...", flush=True)
    peers = {}
    for name, builder in (("mojo", build_mojo), ("c", build_c),
                          ("rust", build_rust)):
        cmd = builder()
        if cmd:
            peers[name] = cmd
    peers["confluent"] = [sys.executable,
                          os.path.join(BENCH, "bench_consume.py")]
    if "mojo" not in peers:
        raise SystemExit("the mojo peer must build for this to mean anything")

    topic = f"mojo-kafka-bench-{int(time.time())}"
    print(f"seeding {args.count} x {args.value_bytes}B into {topic} ...",
          flush=True)
    must(["mojo", "run", "-I", "src", "benchmarks/seed.mojo",
          args.bootstrap, topic, str(args.count), str(args.value_bytes)])

    plan = [
        ("c", "batch"), ("c", "batchhdr"), ("c", "poll"),
        ("mojo", "borrowed"), ("mojo", "consume-nohdr"), ("mojo", "consume"),
        ("mojo", "batch"), ("mojo", "poll"),
        ("rust", "borrowed"), ("rust", "owned"),
        ("confluent", "batch"), ("confluent", "poll"),
    ]
    plan = [(c, m) for c, m in plan if c in peers]

    env = peer_env()
    pin = ["taskset", "-c", args.pin] if args.pin else []
    rates: dict[tuple[str, str], list[float]] = {(c, m): [] for c, m in plan}
    dirty: dict[tuple[str, str], int] = {(c, m): 0 for c, m in plan}
    checksums: set[int] = set()

    for rnd in range(args.rounds):
        for client, mode in plan:
            group = f"{topic}-{client}-{mode}-{rnd}"
            cmd = pin + peers[client] + [
                args.bootstrap, topic, group, mode, str(args.batch),
                str(args.prefetch_ms), str(args.repeat)]
            out = must(cmd, env=env)
            got = RESULT.findall(out)
            if not got:
                sys.stderr.write(out)
                raise SystemExit(f"no RESULT lines from {client}/{mode}")
            for _, _, _, seen, ns, stalls, checksum in got:
                seen, ns, stalls = int(seen), int(ns), int(stalls)
                if seen != args.count:
                    raise SystemExit(
                        f"{client}/{mode} read {seen} of {args.count}; "
                        "the run is not comparable")
                checksums.add(int(checksum))
                if stalls:
                    # Went to the network mid-drain: not a measurement of
                    # the decode path. Discarded, and reported as discarded.
                    dirty[(client, mode)] += 1
                    continue
                rates[(client, mode)].append(seen / (ns / 1e9))
            print(f"  round {rnd + 1} {client:9} {mode:14} "
                  f"{len(rates[(client, mode)]):2d} clean", flush=True)

    if len(checksums) != 1:
        raise SystemExit(
            f"clients disagree on the payload checksum: {sorted(checksums)}; "
            "they are not reading the same thing")

    print()
    print(f"{args.count:,} x {args.value_bytes}B, batch={args.batch}, "
          f"{args.rounds} rounds x {args.repeat} repeats, "
          f"median of clean runs, checksum {checksums.pop()} agreed by all")
    print()
    ceiling = None
    if ("c", "batch") in rates and rates[("c", "batch")]:
        ceiling = statistics.median(rates[("c", "batch")])

    print(f"{'client / mode':<24}{'msg/s':>12}{'ns/msg':>9}"
          f"{'vs C batch':>12}{'min/max':>9}{'n':>4}{'bad':>5}")
    print("-" * 75)
    for client, mode in plan:
        rs = rates[(client, mode)]
        if not rs:
            print(f"{client + ' ' + mode:<24}{'no clean runs':>12}")
            continue
        med = statistics.median(rs)
        frac = f"{med / ceiling:.2f}x" if ceiling else "-"
        print(f"{client + ' ' + mode:<24}{med:>12,.0f}{1e9 / med:>9.1f}"
              f"{frac:>12}{min(rs) / max(rs):>9.2f}"
              f"{len(rs):>4}{dirty[(client, mode)]:>5}")
    print("-" * 75)

    def med(client, mode):
        rs = rates.get((client, mode))
        return statistics.median(rs) if rs else None

    print()
    pairs = [
        ("mojo consume        vs confluent batch", ("mojo", "consume"),
         ("confluent", "batch")),
        ("mojo consume-nohdr  vs confluent batch", ("mojo", "consume-nohdr"),
         ("confluent", "batch")),
        ("mojo consume        vs rust owned", ("mojo", "consume"),
         ("rust", "owned")),
        ("mojo borrowed       vs rust borrowed", ("mojo", "borrowed"),
         ("rust", "borrowed")),
        ("mojo borrowed       vs C batch", ("mojo", "borrowed"),
         ("c", "batch")),
    ]
    for label, a, b in pairs:
        x, y = med(*a), med(*b)
        if x and y:
            print(f"{label:<40} {x / y:.2f}x")


if __name__ == "__main__":
    main()
