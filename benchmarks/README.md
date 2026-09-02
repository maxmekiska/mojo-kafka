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
`--batch 1000 --repeat 5 --rounds 2 --pin 5,6,7`, median of 10 clean runs,
0 discarded, checksum agreed by all four:

| client / mode | msg/s | ns/msg | vs C batch |
|---|---|---|---|
| **c batch** | **7,741,906** | **129.2** | **1.00x** |
| mojo borrowed | 6,542,810 | 152.8 | 0.85x |
| c poll | 4,167,534 | 240.0 | 0.54x |
| c batchhdr | 3,733,818 | 267.8 | 0.48x |
| rust borrowed | 3,162,025 | 316.3 | 0.41x |
| mojo consume-nohdr | 2,865,349 | 349.0 | 0.37x |
| mojo consume | 2,316,982 | 431.6 | 0.30x |
| rust owned | 2,021,552 | 494.7 | 0.26x |
| confluent batch | 1,907,846 | 524.2 | 0.25x |
| mojo batch (`consume_events`) | 1,286,333 | 777.4 | 0.17x |
| mojo poll | 1,203,505 | 830.9 | 0.16x |
| confluent poll | 806,983 | 1239.2 | 0.10x |

### What reproduces, and what does not

Run twice, back to back. **Quote the first two; the third is a range.**

- **`consume_borrowed()` is 85-87% of C.** 0.87x and 0.85x. The tightest
  number in the table, and the one worth caring about: the zero-copy path
  gives up ~24 ns a record against librdkafka driven directly.
- **`consume_borrowed()` is ~2x `rust-rdkafka`'s `BorrowedMessage`.** 1.99x
  and 2.07x. See the caveat below before quoting it as a language result.
- **`consume()` beats `confluent-kafka`'s `consume()`** by 1.26x and 1.21x,
  and **`rust-rdkafka`'s `.detach()`** by 1.07x and 1.15x. Consistent in
  direction, modest in size.
- **`consume(headers=False)` against `confluent-kafka`** measured 2.06x and
  1.50x. The direction is solid, the magnitude is not — **say 1.5x**.

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
| mojo `consume_borrowed()` | c batch, 129.2 | **+23.6 ns** |
| rust `BorrowedMessage` | c poll, 240.0 | **+76.3 ns** |

That comparison still favours this package, and it is the one to make.

### The one that does not favour us

**Our single-record path is the slowest thing we ship.** `mojo poll`
(`poll_event()`) is 830.9 ns a record against `c poll` at 240.0 — an
overhead of ~590 ns, far worse than the borrowed path's 24. `consume_events()`
is nearly as bad at 777.4, and it is *slower than `consume()`* (431.6) doing
strictly more work per record. Both are the same cause: a `PollEvent` is an
`Optional[Message]` beside an `Optional[TopicPartition]`, and materialising
one **into a `List`** costs ~350 ns a record in this Mojo version.

It is specifically the container. Measured with both variants in one binary,
`poll()` decoding straight into its `Optional` came out at 733.7 ns against
720.7 through the `PollEvent` — no gain, because a `PollEvent` that never
reaches a container is elided by the compiler. `consume()` got 1.7x from the
same change precisely because its `PollEvent`s were going into a `List`.

So: **use `consume()` for throughput, and `consume_events()` when you want
end-of-partition marks as entries** — which is the only thing that actually
separates them, since their error policy is identical. It costs about 1.7x.
Making `PollEvent` cheaper to store is the open piece of work here, and is
written up as a TODO in `CLAUDE.md`.

## Files

| | |
|---|---|
| `seed.mojo` | fills a fresh topic; handles `KIND_QUEUE_FULL` by draining and retrying **the same record**, so the count is exact |
| `bench_consume.mojo` | `poll_event`, `consume_events`, `consume`, `consume(headers=False)`, `consume_borrowed` |
| `bench_consume.c` | the ceiling: `rd_kafka_consumer_poll` and `rd_kafka_consume_batch_queue`, plus a `batchhdr` mode that adds the headers call |
| `bench_consume.py` | `confluent-kafka`'s `poll()` and `consume()`, mirrored call for call |
| `rust_peer/` | `rust-rdkafka`'s `BorrowedMessage` and `.detach()`, linked against the conda librdkafka |
| `run.py` | builds the peers, seeds, runs the interleaved plan, checks the checksums, prints the table |
