# Consume benchmark

`consume(n)` against `poll()`, in this package and in `confluent-kafka`.

```bash
pixi run broker-up
pixi run -e interop bench            # --count, --repeat, --batch, --prefetch-ms
pixi run broker-down
```

Local only, like the Docker suites: it needs a real broker and both clients.
CI runs none of it.

## What it does and does not measure

**Both clients wrap the same librdkafka build** — 2.15.0 here, checked with
`confluent_kafka.libversion()`. So nothing below is a protocol or a fetch-path
comparison; the only thing that differs is the binding layer, which is the
layer this package is.

Two methodology choices are load-bearing, and both were arrived at by getting
them wrong first:

- **The timed loop drains a warm local queue.** librdkafka fetches on a
  background thread, so each run pauses `--prefetch-ms` after joining the
  group and before starting the clock, with the queue sizes raised to hold the
  whole topic. Without that pause the benchmark measures fetch latency and
  nothing else: the first version reported all four configurations within 15%
  of each other, at ~37k msg/s, with batching apparently *slower* than
  polling. With it, every number is 10–50x higher and the batch effect is
  plainly visible. If you change the config, re-check that the spread column
  stays tight — a wide spread means the prefetch is not finishing.

- **The median is reported, not the best.** The spread here is not one-sided:
  a run whose prefetch did not complete drains part of the topic off the
  network and comes out several times slower. Best-of-N therefore rewards
  whichever client got luckiest, and an early run of this benchmark had one
  client vary 3.2x across three attempts with its best reported as its speed.

**Read the within-client column first.** "Batching is Nx polling in the same
client" is the claim the design actually makes, and it is measured against a
fixed everything-else. The cross-client number is reported too and is worth
less: it moves with payload size, with how much work the caller does per
record — this loop does none, it counts — and with which client happens to
defer work the other does eagerly.

## Terminating on EOF, not on a count

Both sides set `enable.partition.eof` and stop at the mark. A benchmark that
stopped after *n* messages would let a client that returned fewer look faster,
and the runner additionally asserts every run read exactly the seeded count
before it is allowed into the table.

## What it measured here

WSL2 laptop, Docker broker on localhost, librdkafka 2.15.0, 200k x 100B,
`--batch 1000 --repeat 5 --prefetch-ms 15000`, median:

| client / mode | msg/s | vs own poll | min/max |
|---|---|---|---|
| mojo poll | 594,236 | 1.00x | 0.56 |
| mojo batch | 724,656 | 1.22x | 0.25 |
| **mojo borrowed** | **4,905,405** | **8.25x** | 0.52 |
| confluent poll | 623,311 | 1.00x | 0.41 |
| confluent batch | 1,490,397 | 2.39x | 0.28 |

**Every client computes the same checksum**, and that is the first thing to
check before believing any of it: all three mojo modes and `confluent-kafka`
returned `CHECKSUM 2399952` over the same 50k topic. The borrowed path is not
fast because it skips work.

Because the medians above still carry this machine's noise, the borrowed
claim was also spot-checked back to back in one session on one topic, which
is the cleanest comparison available here:

| | ns for 50k | msg/s |
|---|---|---|
| mojo poll | 76,343,844 | 654,935 |
| mojo batch | 35,666,830 | 1,401,857 |
| confluent batch | 32,168,661 | 1,554,313 |
| **mojo borrowed** | **17,448,781** | **2,865,527** |

Two things reproduce across both:

- **`consume_borrowed()` is about 2x our own `consume()`** -- 17.4ms against
  35.7ms for the same records in the same process. That is the two payload
  copies and the per-record allocations, and it is the clearest signal in
  this whole benchmark because nothing else differs between the two runs.
- **`consume_borrowed()` beats `confluent-kafka`'s `consume()`** by 1.84x in
  the back-to-back check and 3.29x in the 5-repeat median. Both directions
  agree; the magnitude does not, so quote "roughly 2x" and not the larger
  number.

`consume()` against `confluent-kafka`'s `consume()` remains **parity** --
0.90x and 0.49x in two runs of the same configuration, which is a range that
says "the same, plus noise" rather than a deficit. Both copy every key and
value; only the borrowed path does not.

That is the shape you would predict from the code and it is worth stating,
because it is the reason to trust the result: `confluent-kafka` and our owned
path do the same per-message work, so they measure the same. `rust-rdkafka`'s
`BorrowedMessage` does what `consume_borrowed()` does, and that is where the
2x lives -- not in the language, in the copy.

## Files

| | |
|---|---|
| `seed.mojo` | fills a fresh topic; handles `KIND_QUEUE_FULL` by draining and retrying **the same record**, so the count is exact |
| `bench_consume.mojo` | this package's `poll_event()` and `consume_events()` |
| `bench_consume.py` | `confluent-kafka`'s `poll()` and `consume()`, mirrored call for call |
| `run.py` | seeds, runs every combination `--repeat` times, prints the table |
