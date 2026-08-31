"""Run the consume benchmark and print the comparison.

    pixi run broker-up
    pixi run -e interop bench
    pixi run broker-down

Seeds a fresh topic, then times four drains of it: `poll` and `consume(n)`,
in this package and in `confluent-kafka`. Each drain gets its own consumer
group so every one of them reads the whole topic from the beginning.

**The timed loop drains a warm local queue, not the network.** librdkafka
fetches on a background thread, so each run pauses `--prefetch-ms` after
joining the group and before starting the clock, and the queue sizes are set
to hold the whole topic. Without that every configuration measures the same
fetch latency: the first version of this benchmark reported all four within
15% of each other, with batching *slower* than polling, and it was measuring
the broker.

**Read the within-client column first.** Both clients wrap the same
librdkafka build, so the honest claim here is not "Mojo is faster than
Python" in the abstract -- it is what batching buys *within* a client, and
whether the language on the other side of the binding changes that. The
cross-client number is reported too, and is worth less: it moves with
payload size, broker locality and how much work the caller does per record.

Every configuration is timed `--repeat` times and the **median** is reported,
with the full spread beside it. Median rather than best: the spread here is
not one-sided. A run whose prefetch did not finish drains part of the topic
off the network and comes out several times slower, so "best of N" quietly
rewards whichever client got luckiest, and an early version of this
benchmark did exactly that -- one client varied 3.2x across three runs and
its best was reported as if it were its speed. Watch the spread column: if it
is wide, raise `--prefetch-ms` before believing any of it.
"""

import argparse
import os
import re
import statistics
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULT = re.compile(r"^RESULT (\S+) (\S+) (\d+) (\d+)$", re.M)


def run(cmd: list[str]) -> str:
    proc = subprocess.run(
        cmd, cwd=ROOT, capture_output=True, text=True, timeout=1800
    )
    if proc.returncode != 0:
        sys.stderr.write(proc.stdout + proc.stderr)
        raise SystemExit(f"command failed: {' '.join(cmd)}")
    return proc.stdout


def parse(out: str) -> tuple[str, str, int, int]:
    m = RESULT.search(out)
    if not m:
        sys.stderr.write(out)
        raise SystemExit("no RESULT line in output")
    return m.group(1), m.group(2), int(m.group(3)), int(m.group(4))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=os.environ.get(
        "MOJO_KAFKA_BOOTSTRAP", "localhost:9092"))
    ap.add_argument("--count", type=int, default=200_000)
    ap.add_argument("--value-bytes", type=int, default=100)
    ap.add_argument("--batch", type=int, default=1000)
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--prefetch-ms", type=int, default=15000)
    args = ap.parse_args()

    topic = f"mojo-kafka-bench-{int(time.time())}"
    print(f"seeding {args.count} x {args.value_bytes}B into {topic} ...",
          flush=True)
    run(["mojo", "run", "-I", "src", "benchmarks/seed.mojo",
         args.bootstrap, topic, str(args.count), str(args.value_bytes)])

    plans = [
        ("mojo", "poll"),
        ("mojo", "batch"),
        ("mojo", "borrowed"),
        ("confluent", "poll"),
        ("confluent", "batch"),
    ]
    rates: dict[tuple[str, str], list[float]] = {}
    for client, mode in plans:
        rates[(client, mode)] = []
        for attempt in range(args.repeat):
            group = f"{topic}-{client}-{mode}-{attempt}"
            if client == "mojo":
                cmd = ["mojo", "run", "-I", "src",
                       "benchmarks/bench_consume.mojo"]
            else:
                cmd = [sys.executable, "benchmarks/bench_consume.py"]
            cmd += [args.bootstrap, topic, group, mode, str(args.batch),
                    str(args.prefetch_ms)]
            _, _, seen, ns = parse(run(cmd))
            if seen != args.count:
                raise SystemExit(
                    f"{client}/{mode} read {seen} of {args.count} messages; "
                    "the run is not comparable"
                )
            rate = seen / (ns / 1e9)
            rates[(client, mode)].append(rate)
            print(f"  {client:9} {mode:5} attempt {attempt + 1}: "
                  f"{rate:,.0f} msg/s", flush=True)

    print()
    print(f"librdkafka is the same build under both clients; "
          f"{args.count:,} messages x {args.value_bytes}B, "
          f"batch={args.batch}, best of {args.repeat}")
    print()
    def med(client: str, mode: str) -> float:
        return statistics.median(rates[(client, mode)])

    def spread(client: str, mode: str) -> str:
        rs = rates[(client, mode)]
        return f"{min(rs) / max(rs):.2f}"

    print(f"{'client / mode':<22}{'msg/s':>14}{'vs own poll':>14}"
          f"{'min/max':>10}")
    print("-" * 60)
    for client, mode in plans:
        rate = med(client, mode)
        print(f"{client + ' ' + mode:<22}{rate:>14,.0f}"
              f"{rate / med(client, 'poll'):>13.2f}x{spread(client, mode):>10}")
    print("-" * 60)
    print("Every mode reads the first payload byte of every record, so the")
    print("comparison is like for like; `borrowed` reads it through a span")
    print("into librdkafka's buffer, the others through an owned copy.")
    print()
    print(f"batch,    mojo vs confluent: "
          f"{med('mojo', 'batch') / med('confluent', 'batch'):.2f}x")
    print(f"borrowed, mojo vs confluent batch: "
          f"{med('mojo', 'borrowed') / med('confluent', 'batch'):.2f}x")


if __name__ == "__main__":
    main()
