"""Soak: every consume path, produce with headers, and a transaction loop,
each for a wall-clock duration, watching peak RSS for a leak. Local only.

    pixi run soak                       # 120 s per path
    pixi run soak 600                   # the number to quote
    pixi run soak 20 2000               # short, with small rounds -- valgrind
    pixi run soak 60 20000 poll,consume # only these paths

Under valgrind (Linux; `pixi exec` fetches it from conda-forge, no sudo):

    pixi run soak-build
    pixi exec -s valgrind valgrind --leak-check=full --error-exitcode=1 \\
        dist/soak 20 2000

**The claim under test is "no leak per record".** A leak in the headers
walk, the `PollEvent` slot trick or a destructor that skips a field shows
only after hours, which no suite in CI runs for. So each path here runs in
**rounds** of a fixed record count against a fresh `MockCluster`, and peak
RSS is sampled every 10 s after a 20 s warm-up: a round is the same work
every time, so the peak after round one is the peak after round fifty
unless something is kept.

Two things are deliberate about the shape:

- **A fresh cluster per round, not per path.** The mock broker keeps every
  record it is given *in this process*, so one cluster for 600 s would
  grow RSS monotonically with no leak anywhere in the client. Destroying
  it per round is what makes the footprint flat.
- **Every record read is checked against its key.** A decode that goes
  wrong under load -- a stride slip, a freed buffer read back -- is a
  failure here and not merely a leak.

Peak RSS is `getrusage(RUSAGE_SELF).ru_maxrss` through libc, which is
portable to macOS where `/proc` is not; a leak is monotonic, so a peak is
enough. Units differ -- kilobytes on Linux, bytes on macOS -- and are
normalised. Current RSS from `/proc/self/statm` is printed beside it where
it exists, because it is the more sensitive of the two.

**The rule**: fail if the last sample exceeds the first post-warm-up sample
by more than 10% *and* more than 16 MB, on either measure. Print the table
either way. A run shorter than 30 s takes fewer than two samples and
cannot apply the rule; it prints `n/a` and still checks every record.
"""

from std.ffi import OwnedDLHandle
from std.sys import CompilationTarget, argv
from std.time import perf_counter_ns

from kafka import (
    OFFSET_BEGINNING,
    Consumer,
    ConsumerConfig,
    Header,
    KafkaError,
    Message,
    Producer,
    ProducerConfig,
    TopicPartition,
)
from kafka._ffi import Lib
from kafka.testing import MockCluster

comptime TOPIC = "soak"
comptime TOPIC_IN = "soak-in"
comptime TOPIC_OUT = "soak-out"
# 64 bytes of padding, so a record is ~80 bytes and a round moves real
# memory through every buffer on the path.
comptime PAD = (
    "................................................................"
)

comptime WARM_UP_NS = 20_000_000_000
comptime SAMPLE_NS = 10_000_000_000
comptime GROWTH_FLOOR = 16 * 1024 * 1024


# --- records -----------------------------------------------------------------


def _key(i: Int) -> String:
    return "k" + String(i)


def _value(i: Int) -> String:
    return "v" + String(i) + "|" + PAD


def _verify_text(key: String, value: String) raises:
    """`value` must be the one `_value` built for `key`'s index."""
    if key.byte_length() < 2 or key.as_bytes()[0] != UInt8(ord("k")):
        raise Error("record with a malformed key: " + key)
    var expected = "v" + key[byte=1:] + "|" + PAD
    if value != expected:
        raise Error(
            "record " + key + " carried the wrong value: " + value[byte=0:40]
        )


def _verify(message: Message) raises:
    """Presence is checked through `key` / `value`, never `*_text()` --
    those collapse null onto a default, which would hide a lost field."""
    if not message.key:
        raise Error(
            "record with a null key at offset " + String(message.offset)
        )
    if not message.value:
        raise Error(
            "record with a null value at offset " + String(message.offset)
        )
    _verify_text(
        String(unsafe_from_utf8=Span(message.key.value())),
        String(unsafe_from_utf8=Span(message.value.value())),
    )


def _verify_headers(message: Message) raises:
    """The three headers `_fill` writes, in order, null value included."""
    if len(message.headers) != 3:
        raise Error("expected 3 headers, got " + String(len(message.headers)))
    if (
        message.headers[0].name != "h1"
        or message.headers[0].value_text() != "a"
    ):
        raise Error("header 0 is wrong")
    if (
        message.headers[1].name != "h2"
        or message.headers[1].value_text() != "bb"
    ):
        raise Error("header 1 is wrong")
    if message.headers[2].name != "h3" or message.headers[2].value:
        raise Error("header 2 is wrong -- its value must be null")


# --- memory ------------------------------------------------------------------


struct _Rss:
    """Peak and current resident set size, through libc.

    Peak is `ru_maxrss`, at byte offset 32 of `struct rusage` on both Linux
    and macOS (two 16-byte `timeval`s precede it) -- probed with `offsetof`,
    not reasoned about. The buffer is sized for the 144-byte Linux struct
    with room to spare.
    """

    var _libc: OwnedDLHandle

    def __init__(out self) raises:
        var candidates = [
            String("libc.so.6"),
            String("libc.so"),
            String("libSystem.B.dylib"),
        ]
        for name in candidates:
            try:
                self._libc = OwnedDLHandle(name)
                return
            except:
                continue
        raise Error("could not load libc for getrusage")

    def cap_arenas(self) -> Bool:
        """`mallopt(M_ARENA_MAX, 1)`: one malloc arena for the whole process.

        Not a tuning. Every round here starts and ends three librdkafka
        clients, and each client runs a handful of threads; glibc gives new
        threads their own malloc arenas and keeps the freed memory in them,
        so with the default arena policy RSS climbs for minutes and
        plateaus -- measured at +23 MB over 300 s on the `poll` path with
        current RSS still creeping at 400 s -- while a run with
        `MALLOC_ARENA_MAX=1` is flat to within 2 MB over the same path. A
        production process creates its clients once, so that churn is this
        harness's shape and not the client's, and capping the arenas is
        what puts the client's per-record work on the scale rather than the
        allocator's policy. A leak -- memory never freed -- is unaffected
        by the cap and still shows; valgrind is the second, independent
        check for that. `M_ARENA_MAX` is -8 in glibc's malloc.h. Absent on
        macOS, where this is a no-op and the header line says so.
        """
        try:
            var mallopt = self._libc.get_function[Int32]("mallopt")
            return mallopt(Int32(-8), Int32(1)) == 1
        except:
            return False

    def peak_bytes(self) raises -> Int:
        var getrusage = self._libc.get_function[Int32]("getrusage")
        var buf = List[Int](length=32, fill=0)
        var rc = getrusage(Int32(0), Int(buf.unsafe_ptr()))
        var raw = buf[4]
        _ = buf^
        if rc != 0:
            raise Error("getrusage(RUSAGE_SELF) failed")
        if CompilationTarget.is_macos():
            return raw
        return raw * 1024

    def current_bytes(self) -> Int:
        """Current RSS from `/proc/self/statm`, or -1 where that is absent."""
        try:
            var page = Int(self._libc.get_function[Int32]("getpagesize")())
            with open("/proc/self/statm", "r") as f:
                var fields = f.read().split(" ")
                return atol(fields[1]) * page
        except:
            return -1


def _mb(bytes: Int) -> String:
    if bytes < 0:
        return String("n/a")
    var tenths = bytes * 10 // (1024 * 1024)
    return String(tenths // 10) + "." + String(tenths % 10)


def _pad(text: String, width: Int) -> String:
    var out = text
    while out.byte_length() < width:
        out += " "
    return out


# --- one round of each path --------------------------------------------------


def _fill(
    bootstrap: String, topic: String, count: Int, headers: Bool
) raises -> Int:
    """Produce `count` records, flush, and report what is still queued.

    Returns `queue_length()` after the flush -- 0 unless something is
    stuck, which is the other way a long run dies.
    """
    var producer = Producer(ProducerConfig(bootstrap_servers=bootstrap))
    for i in range(count):
        if headers:
            _ = producer.produce(
                topic,
                _value(i),
                key=_key(i),
                headers=[
                    Header("h1", "a"),
                    Header("h2", "bb"),
                    Header("h3", None),
                ],
            )
        else:
            _ = producer.produce(topic, _value(i), key=_key(i))
        if i % 1000 == 999:
            _ = producer.poll(0)
    producer.flush(30000)
    return producer.queue_length()


def _reader(bootstrap: String, name: String) raises -> Consumer:
    """A consumer assigned to the round's partition -- no group join, which
    costs ~3 s on the mock and would dominate a round."""
    var consumer = Consumer(
        ConsumerConfig(
            bootstrap_servers=bootstrap,
            group_id="soak-" + name,
            auto_offset_reset="earliest",
        )
    )
    consumer.assign([TopicPartition(TOPIC, 0, OFFSET_BEGINNING)])
    return consumer^


def _stalled(idle: Int, seen: Int, want: Int, path: String) raises:
    if idle > 30:
        raise Error(path + " stalled at " + String(seen) + "/" + String(want))


def _drain_single(
    mut consumer: Consumer, want: Int, events: Bool
) raises -> Int:
    var seen = 0
    var idle = 0
    while seen < want:
        if events:
            var event = consumer.poll_event(1000)
            if event.message:
                _verify(event.message.value())
                seen += 1
                idle = 0
            else:
                idle += 1
        else:
            var message = consumer.poll(1000)
            if message:
                _verify(message.value())
                seen += 1
                idle = 0
            else:
                idle += 1
        _stalled(idle, seen, want, "poll_event" if events else "poll")
    return seen


def _drain_batch[
    mode: StaticString
](mut consumer: Consumer, want: Int) raises -> Int:
    var seen = 0
    var idle = 0
    while seen < want:
        var got = 0
        comptime if mode == "consume":
            for message in consumer.consume(1000, timeout_ms=1000):
                _verify(message)
                got += 1
        elif mode == "headers":
            for message in consumer.consume(1000, timeout_ms=1000):
                _verify(message)
                _verify_headers(message)
                got += 1
        elif mode == "consume_events":
            for event in consumer.consume_events(1000, timeout_ms=1000):
                if event.message:
                    _verify(event.message.value())
                    got += 1
        else:
            var batch = consumer.consume_borrowed(1000, timeout_ms=1000)
            for i in range(len(batch)):
                var record = batch[i]
                var key = record.key()
                var value = record.value()
                if not key or not value:
                    raise Error("borrowed record with a null field")
                _verify_text(
                    String(unsafe_from_utf8=key.value()),
                    String(unsafe_from_utf8=value.value()),
                )
                got += 1
            _ = batch^
        seen += got
        idle = 0 if got > 0 else idle + 1
        _stalled(idle, seen, want, String(mode))
    return seen


def _check(result: Optional[KafkaError], what: String) raises:
    if result:
        raise Error(what + ": " + String(result.value()))


def _round_transactions(bootstrap: String, count: Int) raises -> Int:
    """One read-process-write round: `count` in, `count` out, offsets sent
    inside every transaction. The one path that joins a group, because
    `send_offsets_to_transaction` needs the consumer's group metadata."""
    var queued = _fill(bootstrap, TOPIC_IN, count, headers=False)

    var ccfg = ConsumerConfig(
        bootstrap_servers=bootstrap,
        group_id="soak-txn-group",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    ccfg.set("isolation.level", "read_committed")
    var consumer = Consumer(ccfg)
    # No wait-for-assignment loop: `consume()` serves the group join itself
    # and returns empty until partitions arrive, which the idle counter
    # below absorbs. The first draft polled until `assignment()` was
    # non-empty and threw the poll's result away -- and the poll that
    # completes the join can also return the first record, so every round
    # came up exactly one short.
    consumer.subscribe([TOPIC_IN])

    var pcfg = ProducerConfig(bootstrap_servers=bootstrap)
    pcfg.set("transactional.id", "soak-txn-id")
    var producer = Producer(pcfg)
    _check(producer.init_transactions(30000), "init_transactions")

    var processed = 0
    var idle = 0
    while processed < count:
        var batch = consumer.consume(500, timeout_ms=1000)
        if len(batch) == 0:
            idle += 1
            _stalled(idle, processed, count, "transactions")
            continue
        idle = 0
        _check(producer.begin_transaction(), "begin_transaction")
        for message in batch:
            _verify(message)
            _ = producer.produce(
                TOPIC_OUT, message.value_text(), key=message.key_text()
            )
        _check(
            producer.send_offsets_to_transaction(
                consumer.position(consumer.assignment()),
                consumer.consumer_group_metadata(),
                10000,
            ),
            "send_offsets_to_transaction",
        )
        _check(producer.commit_transaction(), "commit_transaction")
        processed += len(batch)
    consumer.close()
    return queued


def _one_round(path: String, count: Int) raises -> Int:
    """A fresh cluster, `count` records through `path`, everything torn
    down. Returns the fill producer's post-flush queue length."""
    var cluster = MockCluster()
    var bootstrap = cluster.bootstrap_servers()
    var queued: Int
    if path == "transactions":
        cluster.create_topic(TOPIC_IN, partition_count=1)
        cluster.create_topic(TOPIC_OUT, partition_count=1)
        queued = _round_transactions(bootstrap, count)
    else:
        cluster.create_topic(TOPIC, partition_count=1)
        queued = _fill(bootstrap, TOPIC, count, headers=path == "headers")
        var consumer = _reader(bootstrap, path)
        var seen: Int
        if path == "poll":
            seen = _drain_single(consumer, count, events=False)
        elif path == "poll_event":
            seen = _drain_single(consumer, count, events=True)
        elif path == "consume":
            seen = _drain_batch["consume"](consumer, count)
        elif path == "consume_events":
            seen = _drain_batch["consume_events"](consumer, count)
        elif path == "consume_borrowed":
            seen = _drain_batch["consume_borrowed"](consumer, count)
        elif path == "headers":
            seen = _drain_batch["headers"](consumer, count)
        else:
            raise Error("unknown path " + path)
        if seen != count:
            raise Error(path + " read " + String(seen) + " of " + String(count))
        consumer.close()
    _ = cluster^
    return queued


# --- the run -----------------------------------------------------------------


@fieldwise_init
struct _Row(Copyable, Movable):
    var path: String
    var rounds: Int
    var records: Int
    var seconds: Int
    var first_peak: Int
    var last_peak: Int
    var first_current: Int
    var last_current: Int
    var samples: Int
    var verdict: String


def _grew(first: Int, last: Int) -> Bool:
    """More than 10% *and* more than 16 MB over the baseline."""
    if first < 0 or last < 0:
        return False
    return last * 10 > first * 11 and last - first > GROWTH_FLOOR


def _run_path(path: String, seconds: Int, count: Int, rss: _Rss) raises -> _Row:
    var started = perf_counter_ns()
    var deadline = started + seconds * 1_000_000_000
    var next_sample = started + WARM_UP_NS
    var first_peak = -1
    var last_peak = -1
    var first_current = -1
    var last_current = -1
    var samples = 0
    var rounds = 0
    var records = 0
    var queued = 0
    print("--", path, "for", seconds, "s, rounds of", count)
    while perf_counter_ns() < deadline:
        queued = _one_round(path, count)
        rounds += 1
        records += count
        if perf_counter_ns() >= next_sample:
            var peak = rss.peak_bytes()
            var current = rss.current_bytes()
            if samples == 0:
                first_peak = peak
                first_current = current
            last_peak = peak
            last_current = current
            samples += 1
            next_sample += SAMPLE_NS
            var elapsed = (perf_counter_ns() - started) // 1_000_000_000
            print(
                "   ",
                _pad(String(elapsed) + "s", 6),
                "peak",
                _mb(peak),
                "MB  current",
                _mb(current),
                "MB  records",
                records,
                " producer queue after flush",
                queued,
            )
    var took = (perf_counter_ns() - started) // 1_000_000_000
    var verdict: String
    if samples < 2:
        verdict = String("n/a")
    elif _grew(first_peak, last_peak) or _grew(first_current, last_current):
        verdict = String("FAIL")
    else:
        verdict = String("ok")
    print(
        "   ",
        rounds,
        "rounds,",
        records,
        "records in",
        took,
        "s (",
        records // (took if took > 0 else 1),
        "records/s ) ->",
        verdict,
    )
    return _Row(
        path,
        rounds,
        records,
        Int(took),
        first_peak,
        last_peak,
        first_current,
        last_current,
        samples,
        verdict,
    )


def main() raises:
    var args = argv()
    var seconds = 120
    var count = 20000
    var paths: List[String] = [
        "poll",
        "poll_event",
        "consume",
        "consume_events",
        "consume_borrowed",
        "headers",
        "transactions",
    ]
    if len(args) > 1:
        seconds = atol(args[1])
    if len(args) > 2:
        count = atol(args[2])
    if len(args) > 3:
        paths = List[String]()
        for piece in String(args[3]).split(","):
            paths.append(String(piece))
    var rss = _Rss()
    var capped = rss.cap_arenas()
    # Keep librdkafka mapped for the whole run. Every round drops its last
    # client, and `dlopen` refcounts, so without this the library is
    # unloaded and reloaded once per round -- which is not the shape of any
    # production process and would put OpenSSL's per-load global state,
    # not the client's per-record work, on the scale.
    var pin = Lib()
    print(
        "soak:",
        seconds,
        "s per path, rounds of",
        count,
        "records; start peak",
        _mb(rss.peak_bytes()),
        "MB; malloc arenas capped at 1:",
        capped,
    )

    var rows = List[_Row]()
    for path in paths:
        rows.append(_run_path(path, seconds, count, rss))

    print()
    print(
        _pad("path", 18),
        _pad("rounds", 8),
        _pad("records", 10),
        _pad("peak first", 12),
        _pad("peak last", 12),
        _pad("rss first", 12),
        _pad("rss last", 12),
        "verdict",
    )
    var failed = False
    for row in rows:
        print(
            _pad(row.path, 18),
            _pad(String(row.rounds), 8),
            _pad(String(row.records), 10),
            _pad(_mb(row.first_peak), 12),
            _pad(_mb(row.last_peak), 12),
            _pad(_mb(row.first_current), 12),
            _pad(_mb(row.last_current), 12),
            row.verdict,
        )
        if row.verdict == "FAIL":
            failed = True
    print()
    _ = pin^
    if failed:
        raise Error("soak: RSS grew past the 10% / 16 MB rule on a path above")
    print("soak: every record verified; no path grew past the rule")
