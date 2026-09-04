# Consume benchmark

Four clients, one librdkafka, one topic: **C**, **Rust** (`rust-rdkafka`),
**Mojo** (this package) and **Python** (`confluent-kafka`).

```bash
pixi run broker-up
pixi run -e interop bench          # --count --batch --repeat --rounds --pin
pixi run broker-down
```

Local only, like the Docker suites. CI runs none of it. `run.py` builds the C
and Rust peers itself and **skips** either one if the toolchain is missing,
so the table degrades rather than failing.

## Why four peers

Each one bounds a claim the others cannot.

| peer | what it bounds |
|---|---|
| `c` | The ceiling. Nothing that calls librdkafka can beat it, so "close to native" means "close to this". |
| `rust` | `rust-rdkafka`, a zero-copy binding in a systems language. The peer that makes the borrowed-view claim falsifiable instead of merely flattering — beating a copying Python client proves nothing about a span. |
| `mojo` | This package. |
| `confluent` | `confluent-kafka`, the client this package's API shape follows. |

**Three of the four load the same librdkafka. `confluent-kafka` does not** —
and the previous version of this file claimed otherwise. Its manylinux wheel
bundles its own build: `ldd` on `cimpl…so` names
`librdkafka-c87086af.so.1` out of `confluent_kafka.libs`, not the conda-forge
library. The version string matches (2.15.0); the compiler and flags need
not. So read `mojo`/`c`/`rust` as binding-against-binding, and the
`confluent` row as client-against-client. The Rust peer is pinned to the
conda library on purpose (`features = ["dynamic-linking"]`) so that its row
*is* binding-against-binding.

## Methodology, in the order the mistakes were made

- **Repeats happen inside each peer process**, by seeking back to offset 0
  and re-draining. One drain per process left a spread that swamped every
  effect worth measuring — `CLAUDE.md` recorded a single configuration
  swinging 2.8x between runs, and that is why the owned-path numbers here
  were previously marked untrustworthy.
- **A stalled repeat is discarded, not averaged.** Every peer drains with a
  **zero** timeout and counts the calls that come back empty. If the whole
  topic is already in the local queue, a zero-timeout call cannot come back
  empty before EOF — so a non-zero count means that repeat went to the
  network mid-drain and is measuring the broker. That is a *detector*. It
  replaces the guesswork that `--prefetch-ms` used to be: the pause still
  primes the queue, but nothing rests on having guessed it right, because a
  repeat that stalled is thrown away and reported in the `bad` column.
- **The plan is interleaved across rounds.** A machine that drifts slower
  over time now penalises every configuration equally instead of whichever
  ran last.
- **The median of clean repeats is reported**, with `min/max` beside it.
  Best-of-N rewards whichever client got luckiest.
- **Every peer sums the first payload byte of every record** and prints a
  checksum; `run.py` refuses to print a table unless all four agree. A
  client cannot look fast by skipping the payload.
- **Cross-binary comparisons are not trustworthy; interleaved ones are.**
  Two builds running the *same* work measured 143 ns/record apart here. Every
  number below comes from one interleaved run, and every A/B in the commit
  that produced them was done with both variants compiled into one binary.

## What it measured here

WSL2 laptop, Docker broker on localhost, librdkafka 2.15.0, 200k x 100B,
`--batch 1000 --repeat 7 --rounds 3 --pin 5,6,7`, median of 21 clean runs,
0 discarded, checksum agreed by all four:

| client / mode | msg/s | ns/msg | vs C batch |
|---|---|---|---|
| **c batch** | **6,984,942** | **143.2** | **1.00x** |
| mojo borrowed | 6,201,492 | 161.3 | 0.89x |
| c batchhdr | 3,680,517 | 271.7 | 0.53x |
| c poll | 3,240,118 | 308.6 | 0.46x |
| rust borrowed | 2,870,823 | 348.3 | 0.41x |
| mojo consume-nohdr | 2,717,384 | 368.0 | 0.39x |
| mojo consume | 1,971,575 | 507.2 | 0.28x |
| rust owned | 1,687,215 | 592.7 | 0.24x |
| confluent batch | 1,613,296 | 619.8 | 0.23x |
| mojo batch (`consume_events`) | 1,223,805 | 817.1 | 0.18x |
| mojo poll-nohdr | 1,031,813 | 969.2 | 0.15x |
| mojo poll | 846,479 | 1181.4 | 0.12x |
| confluent poll | 848,569 | 1178.5 | 0.12x |

**This machine was 11-17% slower than the run the previous table came from**
(`c batch` 143.2 against 129.2, `mojo consume` 507.2 against 431.6), so do
not read any row against the old one -- that is exactly the cross-run
comparison this file says not to make. Every claim below is either a ratio
*within* this run or an interleaved in-binary A/B.

### What reproduces, and what does not

Run twice, back to back. **Quote the first two; the third is a range.**

- **`consume_borrowed()` is 85-89% of C.** 0.87x, 0.85x and 0.89x over three
  runs. The tightest number in the table, and the one worth caring about:
  the zero-copy path gives up 18-24 ns a record against librdkafka driven
  directly.
- **`consume_borrowed()` is 2.0-2.2x `rust-rdkafka`'s `BorrowedMessage`.**
  1.99x, 2.07x, 2.16x. See the caveat below before quoting it as a language
  result.
- **`consume()` beats `confluent-kafka`'s `consume()`** by 1.26x, 1.21x and
  1.22x, and **`rust-rdkafka`'s `.detach()`** by 1.07x, 1.15x and 1.17x.
  Consistent in direction, modest in size.
- **`consume(headers=False)` against `confluent-kafka`** measured 2.06x,
  1.50x and 1.68x. The direction is solid, the magnitude is not — **say
  1.5x**.

**Quote the default `--count 200000` configuration and nothing smaller.** The
cross-client ratios inflate badly on short runs: at `--count 50000 --repeat 2`
the same `mojo consume` vs `confluent batch` pair measured **3.02x** against
1.21x at 200k, because fixed per-process cost is amortised over a quarter of
the records and `confluent-kafka` carries the most of it. Short runs are for
checking that the harness works, not for numbers.

### The caveat on the Rust rows

**`rust-rdkafka` exposes no batch consume**, so `rust borrowed` and
`rust owned` are one `rd_kafka_consumer_poll` per record while the Mojo rows
batch. The C peer measures exactly what that is worth: `c batch` 129.2 against
`c poll` 240.0, so the fetch mode alone is ~111 ns a record. Most of the 2x
is the API, not the language.

Measured against C doing **the same access pattern**, which is the honest
binding-to-binding comparison:

| binding | its C baseline | overhead per record |
|---|---|---|
| mojo `consume_borrowed()` | c batch, 143.2 | **+18.1 ns** |
| rust `BorrowedMessage` | c poll, 308.6 | **+39.7 ns** |

That comparison still favours this package, and it is the one to make.

### The one that does not favour us

**The single-record path is the slowest thing we ship, and `rust-rdkafka`
beats it.** This is the honest apples-to-apples comparison, and `run.py`'s
summary line does *not* make it: it pairs `mojo consume` against `rust
owned`, which is our batch API against their per-record one. Compared like
for like — owned record, one call each — Rust wins:

| owned, per record | ns/msg |
|---|---|
| c poll | 308.6 |
| rust `.detach()` | 592.7 |
| **mojo `poll_event(headers=False)`** | **969.2** |
| mojo `poll_event()` | 1181.4 |
| confluent `poll()` | 1178.5 |

So we are ~1.6x Rust on the owned single-record path even with headers off,
and level with `confluent-kafka` without. Our wins are the batch path, which
`rust-rdkafka` does not offer at all, and the borrowed path, where we are
2.16x it.

**`consume_events()` costs ~1.6x `consume()`** (817.1 against 507.2 in this
run, down from ~1.8x before the change below), and the remainder is the
container: a `PollEvent` is an `Optional[Message]` beside an
`Optional[TopicPartition]`, 208 bytes measured, against 144 for a `Message`.

**What was done about it, and what was tried and rejected**, all interleaved
in one binary:

- **Constructing the `PollEvent` into the list's slot** instead of building
  it on the stack and moving it in: **-66, -112 and -74 ns a record** over
  three runs. Shipped.
- **A lazy container** storing `List[Message]` and building the `PollEvent`
  on demand: 713.1 against the shipped 673.3 — **worse**. The 64-byte
  footprint saving is given back by the move-out at access time, which means
  the footprint was never the problem. It also means no public type had to
  change.
- **Assigning into the slot through a `ref`**: worth nothing (786.9 against
  781.0). A move-assign destroys the old value first, so the win is
  construction-in-place specifically.
- **The same trick on `consume()`**: 467.9 against 426.9 — **worse**.
  `Message` has no free default, so the placeholder costs more than the move
  it saves.
- **Caching `rd_kafka_topic_name` across `poll_event()` calls** — the
  suspect this file used to name for the poll gap: 1119.3 against 904.3, a
  **215 ns regression**. The hypothesis is tested and false.
- **`poll(headers=False)` / `poll_event(headers=False)`**, the escape hatch
  `consume()` already had: **-75, -157 and -212 ns** across three runs.
  Shipped.

So: **use `consume()` for throughput, `consume_events()` when you want
end-of-partition marks as entries** — the only thing that actually separates
them, since their error policy is identical — and pass `headers=False` on
either whenever the records carry none.

## Files

| | |
|---|---|
| `seed.mojo` | fills a fresh topic; handles `KIND_QUEUE_FULL` by draining and retrying **the same record**, so the count is exact |
| `bench_consume.mojo` | `poll_event`, `poll_event(headers=False)`, `consume_events`, `consume`, `consume(headers=False)`, `consume_borrowed` |
| `bench_consume.c` | the ceiling: `rd_kafka_consumer_poll` and `rd_kafka_consume_batch_queue`, plus a `batchhdr` mode that adds the headers call |
| `bench_consume.py` | `confluent-kafka`'s `poll()` and `consume()`, mirrored call for call |
| `rust_peer/` | `rust-rdkafka`'s `BorrowedMessage` and `.detach()`, linked against the conda librdkafka |
| `run.py` | builds the peers, seeds, runs the interleaved plan, checks the checksums, prints the table |
