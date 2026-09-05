"""Smoke tests -- no broker required.

Run with `pixi run test`. These catch FFI breakage: if librdkafka cannot
be loaded, or a symbol has moved, the first test fails loudly rather than
at the first produce in someone's pipeline.
"""

from std.os import getenv, setenv
from std.atomic import Atomic
from std.ffi import OwnedDLHandle
from std.time import perf_counter_ns, sleep
from std.testing import TestSuite, assert_equal, assert_true

from kafka._ffi import PTR_STRIDE, RD_KAFKA_RESP_ERR__TRANSPORT, _c_string
from kafka.consumer import MAX_BATCH
from kafka.consumer import (
    Rebalance,
    RebalanceHandler,
    _handler_from_word,
    _handler_word,
)
from kafka.producer import _DrState
from kafka.telemetry import RETAINED, _error_trampoline
from kafka import (
    KIND_AUTHORIZATION,
    KIND_FATAL,
    KIND_MESSAGE_TOO_LARGE,
    KIND_OTHER,
    KIND_QUEUE_FULL,
    KIND_TIMED_OUT,
    KIND_TRANSPORT,
    KIND_UNKNOWN_TOPIC_OR_PARTITION,
    TXN_ABORT,
    TXN_FATAL,
    TXN_RETRY,
    Consumer,
    ConsumerConfig,
    Producer,
    ProducerConfig,
    KafkaError,
    TopicPartition,
    kind_of,
    librdkafka_version,
)


def test_librdkafka_loadable() raises:
    """Loads the shared library and calls into it."""
    var v = librdkafka_version()
    assert_true(len(v.codepoints()) > 0, "empty librdkafka version string")
    print("    librdkafka", v)


def test_producer_config_defaults() raises:
    var cfg = ProducerConfig(bootstrap_servers="localhost:9092")
    assert_equal(cfg.client_id, "mojo-kafka")
    assert_equal(cfg.acks, "all")
    assert_equal(cfg.compression_type, "none")


def test_consumer_config_defaults() raises:
    var cfg = ConsumerConfig(bootstrap_servers="localhost:9092", group_id="g")
    assert_equal(cfg.group_id, "g")
    assert_equal(cfg.auto_offset_reset, "latest")
    assert_true(cfg.enable_auto_commit, "auto commit should default on")
    # Off by default, matching librdkafka: a tail-following consumer would
    # otherwise be handed an end-of-partition mark every time it caught up.
    assert_true(
        not cfg.enable_partition_eof, "partition EOF should default off"
    )


def test_produce_accepts_an_explicit_timestamp() raises:
    """Guards the `_VuArray` entry count, without needing a broker.

    Adding the timestamp took the array from seven `rd_kafka_vu_t` entries
    to eight. `_VuArray` fixes its capacity at construction and `_entry`
    raises rather than overrunning, so a forgotten `_VuArray(7)` fails on
    the first produce -- here, loudly, rather than by corrupting the array
    in somebody's pipeline.

    Nothing listens on port 9, so this asserts only that the message was
    *enqueued*; whether the broker preserved the timestamp is covered on the
    mock and against a real broker.
    """
    var cfg = ProducerConfig(bootstrap_servers="127.0.0.1:9")
    cfg.set("message.timeout.ms", "300")
    cfg.set("log_level", "0")
    var p = Producer(cfg)

    # A named time, and the 0 default that means "stamp it now".
    var stamped = p.produce(topic="t", value="v", timestamp=1600000000000)
    var now = p.produce(topic="t", value="v")
    var payload: List[UInt8] = [1]
    var raw = p.produce_bytes(topic="t", value=payload^, timestamp=Int64(42))
    assert_true(stamped != now, "sequences collided")
    assert_true(raw != now, "sequences collided")

    try:
        p.flush(3000)
    except:
        pass
    _ = p.take_failures()
    print("    8-entry vu array accepted a timestamp")


def test_drain_timeout_bounds_the_drop() raises:
    """`drain_timeout_ms` is how long dropping a producer waits, not 5 s.

    A message with a 5 s `message.timeout.ms` is enqueued at a dead port,
    so the destructor's flush has something to wait for; with
    `drain_timeout_ms=300` the drop must return in well under the 5 s it
    would otherwise take. The default is asserted separately so a change
    to it is a deliberate one.
    """
    assert_equal(
        ProducerConfig(bootstrap_servers="127.0.0.1:9").drain_timeout_ms, 5000
    )
    var cfg = ProducerConfig(
        bootstrap_servers="127.0.0.1:9", drain_timeout_ms=300
    )
    cfg.set("message.timeout.ms", "5000")
    cfg.set("log_level", "0")
    var started = perf_counter_ns()
    var p = Producer(cfg)
    _ = p.produce(topic="nowhere", value="v")
    _ = p^
    var took_ms = (perf_counter_ns() - started) // 1_000_000
    assert_true(
        took_ms < 2500,
        "dropping took " + String(took_ms) + "ms; drain_timeout_ms ignored",
    )
    print("    drop returned after", took_ms, "ms with drain_timeout_ms=300")


def test_extra_keys_recorded() raises:
    var cfg = ProducerConfig(bootstrap_servers="localhost:9092")
    cfg.set("message.max.bytes", "1000000")
    assert_equal(cfg.extra["message.max.bytes"], "1000000")


def test_set_passes_keys_verbatim() raises:
    """`log_level` is a real librdkafka property, underscore and all.

    `set()` used to rewrite `_` to `.`, which turned this into an invalid
    `log.level` and put the property permanently out of reach.
    """
    var cfg = ProducerConfig(bootstrap_servers="127.0.0.1:9")
    cfg.set("log_level", "0")
    var p = Producer(cfg)  # raises if librdkafka rejected the key
    _ = p.poll(0)


def test_flush_reports_undelivered_messages() raises:
    """An undelivered message must never look like a successful one.

    Nothing listens on port 9, so every message is dropped once
    `message.timeout.ms` expires. That empties the local queue, and before
    delivery reports were wired up `flush()` saw an empty queue and returned
    cleanly -- losing three messages without a word.
    """
    var cfg = ProducerConfig(bootstrap_servers="127.0.0.1:9")
    cfg.set("message.timeout.ms", "1500")
    cfg.set("log_level", "0")
    var p = Producer(cfg)
    var sent = List[Int]()
    for i in range(3):
        sent.append(
            p.produce(
                topic="nowhere", key="k" + String(i), value="v" + String(i)
            )
        )

    var raised = False
    try:
        p.flush(10000)
    except e:
        raised = True
        var text = String(e)
        assert_true(
            text.find("failed delivery") >= 0, "unexpected error: " + text
        )
        print("    flush reported:", text)
    assert_true(raised, "flush() reported success for 3 lost messages")

    # The reports must survive the raise -- discarding them there would put
    # the caller back to a count and a string.
    assert_equal(len(p.failures()), 3)

    var reports = p.take_failures()
    assert_equal(len(reports), 3)
    for i in range(3):
        # Each verdict is addressable: it carries the token produce() handed
        # back, in the order the messages were enqueued.
        assert_equal(reports[i].sequence, sent[i])
        assert_equal(reports[i].topic, "nowhere")
        assert_true(
            reports[i].error != "", "a failure report with no error text"
        )
    print("    per-message reports:", reports[0])

    # Taking them is what acknowledges them.
    assert_equal(len(p.failures()), 0)


def test_produce_sequences_are_distinct() raises:
    """Every message gets its own token, and none of them is 0.

    The opaque travels as a `void *` and returns as `_private`, where 0 is
    indistinguishable from a message produced without one -- so sequences
    start at 1.
    """
    var cfg = ProducerConfig(bootstrap_servers="127.0.0.1:9")
    cfg.set("message.timeout.ms", "300")
    cfg.set("log_level", "0")
    var p = Producer(cfg)

    var seen = List[Int]()
    for i in range(5):
        var seq = p.produce(topic="nowhere", value="v" + String(i))
        assert_true(seq != 0, "sequence 0 is indistinguishable from no opaque")
        for prior in seen:
            assert_true(prior != seq, "sequence " + String(seq) + " reused")
        seen.append(seq)

    try:
        p.flush(5000)
    except:
        pass
    _ = p.take_failures()
    print("    5 distinct tokens, first was", seen[0])


def test_error_kinds_classify_the_codes_callers_branch_on() raises:
    """`kind_of` groups librdkafka codes into branchable categories.

    Several codes collapse onto one kind deliberately, so this asserts the
    grouping rather than a one-to-one mapping -- and asserts that an
    unclassified code lands on OTHER instead of being mistaken for a
    neighbour.
    """
    assert_true(kind_of(-184) == KIND_QUEUE_FULL, "__QUEUE_FULL")

    # Both timeout codes are one kind: a caller retrying does not care which.
    assert_true(kind_of(-185) == KIND_TIMED_OUT, "__TIMED_OUT")
    assert_true(kind_of(-192) == KIND_TIMED_OUT, "__MSG_TIMED_OUT")

    # Connectivity, likewise.
    assert_true(kind_of(-195) == KIND_TRANSPORT, "__TRANSPORT")
    assert_true(kind_of(-187) == KIND_TRANSPORT, "__ALL_BROKERS_DOWN")

    # Local and broker-side "no such topic/partition" are the same problem.
    assert_true(
        kind_of(-188) == KIND_UNKNOWN_TOPIC_OR_PARTITION, "__UNKNOWN_TOPIC"
    )
    assert_true(
        kind_of(-190) == KIND_UNKNOWN_TOPIC_OR_PARTITION, "__UNKNOWN_PART"
    )
    assert_true(
        kind_of(3) == KIND_UNKNOWN_TOPIC_OR_PARTITION, "UNKNOWN_TOPIC_OR_PART"
    )

    assert_true(kind_of(10) == KIND_MESSAGE_TOO_LARGE, "MSG_SIZE_TOO_LARGE")
    assert_true(kind_of(-169) == KIND_AUTHORIZATION, "__AUTHENTICATION")
    assert_true(kind_of(29) == KIND_AUTHORIZATION, "TOPIC_AUTHORIZATION_FAILED")
    assert_true(kind_of(-150) == KIND_FATAL, "__FATAL")
    assert_true(kind_of(-144) == KIND_FATAL, "__FENCED")

    # Anything not called out is OTHER, including success.
    assert_true(kind_of(0) == KIND_OTHER, "NO_ERROR")
    assert_true(kind_of(22) == KIND_OTHER, "an uncategorised broker code")

    # Distinct kinds must not compare equal -- the whole point of the type.
    assert_true(KIND_QUEUE_FULL != KIND_TIMED_OUT, "kinds collapsed")
    print("    kinds classify; QUEUE_FULL prints as", String(KIND_QUEUE_FULL))


def test_txn_action_orders_abort_before_fatal() raises:
    """The three-way transactional branch, in librdkafka's order.

    This is the case the order exists for: librdkafka flags an error **both**
    fatal and abortable, because the transactional producer treats most of
    the idempotent producer's fatal errors as recoverable -- a transaction
    can be aborted and replayed whole. Testing `is_fatal` first reads
    correctly and answers `TXN_FATAL`, which tears down a producer that only
    needed `abort_transaction()`.

    Built from values rather than from a real error on purpose: no broker,
    and every cell of the table is reachable, including the both-flags one
    that a live cluster produces only under fencing.
    """
    # abortable alone.
    assert_true(
        KafkaError(-1, "abortable", txn_requires_abort=True).txn_action()
        == TXN_ABORT,
        "an abortable error must abort",
    )

    # Fatal *and* abortable -- the ordering case. Abort wins.
    assert_true(
        KafkaError(
            -1, "both", is_fatal=True, txn_requires_abort=True
        ).txn_action()
        == TXN_ABORT,
        "is_fatal was tested before txn_requires_abort",
    )

    # Retriable and abortable. Abort still wins: retrying a call that needs
    # an abort leaves the transaction wedged.
    assert_true(
        KafkaError(
            -1, "both", is_retriable=True, txn_requires_abort=True
        ).txn_action()
        == TXN_ABORT,
        "is_retriable was tested before txn_requires_abort",
    )

    # Retriable alone.
    assert_true(
        KafkaError(-1, "retriable", is_retriable=True).txn_action()
        == TXN_RETRY,
        "a retriable error must be retried",
    )

    # Fatal alone.
    assert_true(
        KafkaError(-1, "fatal", is_fatal=True).txn_action() == TXN_FATAL,
        "a fatal error must be fatal",
    )

    # No flags at all. librdkafka's guidance is explicit -- "treat all other
    # errors as fatal" -- so this is not an "unknown" fourth tag.
    assert_true(
        KafkaError(-1, "unflagged").txn_action() == TXN_FATAL,
        "an unflagged error must be treated as fatal",
    )

    assert_true(TXN_ABORT != TXN_FATAL, "actions collapsed")
    print("    txn_action orders abort first; prints as", String(TXN_ABORT))


def test_a_bare_error_code_claims_no_flags() raises:
    """A `KafkaError` built from a code alone must not assert anything.

    The flags live on `rd_kafka_error_t`, which most of this API never sees;
    a bare `rd_kafka_resp_err_t` has nowhere to keep one. False here is the
    absence of an opinion, not a claim that the operation was survivable --
    which is why `txn_action()` is documented as meaningful only for an
    error a transactional call returned.
    """
    var err = KafkaError(-185, "Local: Timed out")
    assert_true(not err.is_fatal, "a bare code claimed to be fatal")
    assert_true(not err.is_retriable, "a bare code claimed to be retriable")
    assert_true(
        not err.txn_requires_abort, "a bare code claimed to need an abort"
    )
    # The code still classifies, which is the part a bare code *can* answer.
    assert_true(err.kind() == KIND_TIMED_OUT, "kind lost")


def test_take_error_reads_the_flags_off_a_real_error() raises:
    """The flags must survive `take_error`, which destroys what holds them.

    Pure-value tests above cover the branch order but not the read: the
    predicates are called on a `rd_kafka_error_t*` that is freed three lines
    later, so dropping them, reading them after the destroy, or never
    calling them at all all look identical from `KafkaError` alone.

    `init_transactions` against a dead port is the cheapest flagged error this
    package can raise without a broker -- probed, not assumed: seek and
    `sasl_set_credentials` return NULL outright, and every other call
    answers with a bare code. librdkafka answers this one
    `__TIMED_OUT` with the **retriable** flag set, in the timeout it was
    given, so the whole case costs 500ms and no Docker.
    """
    var cfg = ProducerConfig(bootstrap_servers="127.0.0.1:9")
    cfg.set("transactional.id", "mojo-kafka-smoke-txn")
    cfg.set("log_level", "0")
    var p = Producer(cfg)

    var failure = p.init_transactions(500)
    assert_true(
        Bool(failure), "init_transactions succeeded against a dead port"
    )
    ref err = failure.value()
    assert_equal(err.code, -185, "expected __TIMED_OUT: " + String(err))
    assert_true(
        err.is_retriable,
        "the retriable flag was lost crossing take_error: " + String(err),
    )
    assert_true(not err.is_fatal, "a timeout is not fatal")
    assert_true(not err.txn_requires_abort, "a timeout needs no abort")
    assert_true(
        err.txn_action() == TXN_RETRY,
        "a retriable timeout must be retried, not " + String(err.txn_action()),
    )
    assert_true(err.message != "", "no error string")
    print("    real flagged error:", err, "->", err.txn_action())


def test_a_producer_without_a_transactional_id_is_not_retriable() raises:
    """The other half: an error librdkafka leaves entirely unflagged.

    Without `transactional.id` the call fails immediately with
    `__NOT_CONFIGURED` and no flags at all, which `txn_action()` must call
    fatal rather than retry -- retrying would spin forever on a producer
    whose configuration can never satisfy the call. It also pins the flags
    as genuinely read per error, not stamped on by `take_error`.
    """
    var cfg = ProducerConfig(bootstrap_servers="127.0.0.1:9")
    cfg.set("log_level", "0")
    var p = Producer(cfg)

    var failure = p.init_transactions(500)
    assert_true(Bool(failure), "init_transactions succeeded with no txn id")
    ref err = failure.value()
    assert_equal(err.code, -145, "expected __NOT_CONFIGURED: " + String(err))
    assert_true(not err.is_retriable, "a misconfiguration is not retriable")
    assert_true(
        err.txn_action() == TXN_FATAL,
        "an unflagged error must be fatal, not " + String(err.txn_action()),
    )


def _slot_marker_a(event: Rebalance) raises:
    _ = setenv("MOJO_KAFKA_SLOT_MARKER", "A", overwrite=True)


def _slot_marker_b(event: Rebalance) raises:
    _ = setenv("MOJO_KAFKA_SLOT_MARKER", "B", overwrite=True)


def test_a_handler_slot_is_one_word() raises:
    """A rebalance handler slot must be a single naturally-aligned word.

    This is the guard that replaced an untestable lock, and it is the same
    kind of guard as the struct-stride ones: it asserts the *shape* the
    safety argument depends on, because the race it protects against cannot
    be provoked on demand.

    `subscribe()` writes these slots and the rebalance trampoline reads them
    on another thread. One word means one atomic store and one atomic load,
    so a reader sees the whole old address or the whole new one. They used to
    hold `Optional[RebalanceHandler]`, guarded by a `_Latch`, on the recorded
    reasoning that a slot was "pointer-sized and aligned" and so could not
    tear. **Measured here, that was false** -- an `Optional` is a
    discriminant *plus* the pointer -- so an unguarded read could pair
    `has_value = True` with the other value's payload and call through it.

    So both halves are asserted: the bare handler is one word, and the
    `Optional` is not. Put a multi-word type back in those slots and this
    fails.
    """
    var bare = List[RebalanceHandler](capacity=2)
    bare.append(_slot_marker_a)
    bare.append(_slot_marker_b)
    var bare_stride = Int(bare.unsafe_ptr().unsafe_offset(1)) - Int(
        bare.unsafe_ptr()
    )
    assert_equal(
        bare_stride,
        PTR_STRIDE,
        "a handler is no longer a single word; the slots cannot stay atomic",
    )

    var boxed = List[Optional[RebalanceHandler]](capacity=2)
    boxed.append(Optional[RebalanceHandler](_slot_marker_a))
    boxed.append(Optional[RebalanceHandler](_slot_marker_b))
    var boxed_stride = Int(boxed.unsafe_ptr().unsafe_offset(1)) - Int(
        boxed.unsafe_ptr()
    )
    assert_true(
        boxed_stride > PTR_STRIDE,
        (
            "Optional[RebalanceHandler] is now one word -- re-check"
            " _RebalanceState, the reasoning there assumes it is not"
        ),
    )
    print(
        "    handler slot:",
        bare_stride,
        "bytes; Optional would be",
        boxed_stride,
    )


def test_a_handler_survives_the_round_trip_through_its_slot() raises:
    """The pun the slots rest on must reach the *same* function.

    A slot holds a raw code address, so `_handler_word` / `_handler_from_word`
    are what stand between an atomic store and calling the right handler.
    Two distinct handlers, because a round trip that always returned the
    first one would pass with a single one.
    """
    var word_a = _handler_word(_slot_marker_a)
    var word_b = _handler_word(_slot_marker_b)
    assert_true(word_a != 0 and word_b != 0, "a handler address was 0")
    assert_true(word_a != word_b, "two handlers share one address")

    var event = Rebalance(List[TopicPartition](), False, 0, 0, 0)

    _ = setenv("MOJO_KAFKA_SLOT_MARKER", "", overwrite=True)
    _handler_from_word(word_a)(event)
    assert_equal(
        getenv("MOJO_KAFKA_SLOT_MARKER"),
        "A",
        "the slot called the wrong handler",
    )

    _handler_from_word(word_b)(event)
    assert_equal(
        getenv("MOJO_KAFKA_SLOT_MARKER"),
        "B",
        "the slot called the wrong handler",
    )
    print("    handler round-tripped through its slot and kept its identity")


def test_delivery_reports_carry_a_branchable_kind() raises:
    """A rejection's kind is readable without matching on error text.

    Nothing listens on port 9, so every message times out. The report must
    say TIMED_OUT as a value, which is what makes a retry policy expressible.
    """
    var cfg = ProducerConfig(bootstrap_servers="127.0.0.1:9")
    cfg.set("message.timeout.ms", "800")
    cfg.set("log_level", "0")
    var p = Producer(cfg)
    for i in range(2):
        _ = p.produce(topic="nowhere", value="v" + String(i))
    try:
        p.flush(10000)
    except:
        pass

    var reports = p.take_failures()
    assert_equal(len(reports), 2)
    for report in reports:
        assert_true(
            report.kind() == KIND_TIMED_OUT,
            "expected TIMED_OUT, got " + String(report.kind()),
        )
        # The exact code is still there when it matters.
        assert_equal(report.error_code, Int32(-192))
    print("    both rejections classified as", String(reports[0].kind()))


def test_queue_full_is_branchable_backpressure() raises:
    """The case this whole item exists for.

    `confluent-kafka-python` raises `BufferError` for `__QUEUE_FULL` and
    nothing else, so a caller can drain and retry rather than parsing error
    text. Mojo cannot type an exception, so the kind is read back from the
    producer instead.

    A one-message local queue pointed at a dead broker fills on the second
    produce, since nothing is ever acknowledged to free a slot.
    """
    var cfg = ProducerConfig(bootstrap_servers="127.0.0.1:9")
    cfg.set("queue.buffering.max.messages", "1")
    cfg.set("message.timeout.ms", "30000")
    cfg.set("log_level", "0")
    var p = Producer(cfg)

    _ = p.produce(topic="nowhere", value="fills-the-queue")

    var refused = False
    for _ in range(10):
        try:
            _ = p.produce(topic="nowhere", value="one-too-many")
        except:
            refused = True
            break
    assert_true(refused, "a 1-message queue accepted more than one message")

    assert_true(
        p.last_error_kind() == KIND_QUEUE_FULL,
        "expected QUEUE_FULL, got " + String(p.last_error_kind()),
    )
    print("    backpressure surfaced as", String(p.last_error_kind()))
    _ = p.take_failures()


# --- concurrent producer ----------------------------------------------------
#
# `Producer` claims to tolerate being driven from more than one thread, and
# that claim is worth exactly as much as a test that actually does it. Mojo
# 1.0 ships no thread API -- there is no `std.sync` and no `parallelize`
# outside MAX, which this package deliberately does not depend on -- so the
# threads come from libc through the same `abi("C")` mechanism the rebalance
# and delivery callbacks use.
#
# Nothing listens on port 9, so every message fails at `message.timeout.ms`.
# That is the point: a *delivered* message never touches the failure list,
# because the trampoline's success path returns before it. Only a failing
# one exercises the latch, and only many failing ones arriving from several
# threads at once exercise it under contention.

comptime _THREADS = 8
comptime _PER_THREAD = 400
comptime _TOTAL = _THREADS * _PER_THREAD


def _open_libc() raises -> OwnedDLHandle:
    """libc, for `pthread_create` / `pthread_join`.

    Same candidate-list shape as `_open_librdkafka`, for the same reason:
    the name differs per platform and CI runs both. glibc 2.34 folded the
    pthread symbols into libc proper, and on macOS they have always lived
    in libSystem.
    """
    var candidates = [
        String("libc.so.6"),
        String("libc.so"),
        String("libSystem.B.dylib"),
    ]
    for name in candidates:
        try:
            return OwnedDLHandle(name)
        except:
            continue
    raise Error("could not load libc for pthread_create")


@fieldwise_init
struct _Work(Copyable, Movable):
    """What one producing thread needs. Passed as `pthread_create`'s `void *`.

    Addresses rather than references: a thin C callback captures nothing, so
    this struct is the entire world the thread body can see.
    """

    var producer: Int
    var tokens: Int
    var start: Int
    var count: Int


def _produce_worker(arg: Int) abi("C") -> Int:
    """Produce `count` messages, recording each token in its own slot.

    The slots are disjoint per thread, so the token array needs no lock --
    the contention this test is looking for is inside `Producer`, and a
    lock out here would only mask it.

    `poll(0)` between produces is what puts the delivery-report callback on
    *this* thread as well as the main one, which is the case `_Latch` exists
    for: librdkafka runs the callback on whichever thread called poll.
    """
    try:
        ref work = Pointer[_Work, ImmutAnyOrigin](unsafe_from_address=arg)[
            unsafe_offset=0
        ]
        ref producer = Pointer[Producer, MutAnyOrigin](
            unsafe_from_address=work.producer
        )[unsafe_offset=0]
        var slots = Pointer[Int, MutAnyOrigin](unsafe_from_address=work.tokens)
        for i in range(work.count):
            var at = work.start + i
            slots[unsafe_offset=at] = producer.produce(
                topic="nowhere", value="c-" + String(at)
            )
            if i % 8 == 7:
                _ = producer.poll(0)
    except:
        # A thread body may not raise across the C boundary. A failure here
        # leaves this thread's slots at 0, which the assertions below catch
        # as a missing token rather than passing quietly.
        pass
    return 0


def test_concurrent_produce_keeps_every_sequence_and_report() raises:
    """Four threads producing at once must not lose or duplicate a message.

    Two pieces of `Producer` state are shared, and this asserts on both:

    - **The sequence counter.** It is an `Atomic`, so `fetch_add` hands each
      message its own token. A plain `+= 1` loses increments under
      contention, and two messages sharing a token is not a cosmetic bug --
      it mis-attributes their delivery reports to each other. Asserting the
      tokens are exactly `1 ..= _TOTAL` catches both a duplicate and a gap.

    - **The failure list.** Every one of these messages times out, so all
      `_TOTAL` reports are appended through `_Latch` from whichever threads
      happen to be inside `poll` / `flush`, while the main thread reads the
      running count. An unguarded `List` reallocating under a concurrent
      append is a corrupted heap, not a wrong number.

    The thread and message counts are **measured, not guessed.** Reverting
    the producer to a plain `+= 1` and no latch, this fails 6 runs out of 6
    at 8x400; at the 4x50 it was first written with it caught the same
    regression only 3 times in 6, which is a guard that lets half of them
    through. Do not lower them without re-measuring the same way.
    """
    var libc = _open_libc()
    var create = libc.get_function[Int32]("pthread_create")
    var join = libc.get_function[Int32]("pthread_join")

    var cfg = ProducerConfig(bootstrap_servers="127.0.0.1:9")
    cfg.set("message.timeout.ms", "2000")
    cfg.set("log_level", "0")
    var producer = Producer(cfg)

    var tokens = List[Int](length=_TOTAL, fill=0)
    # Sized once and never appended to: `_Work` addresses are handed to
    # threads, and a reallocating append would leave every one of them
    # dangling -- the `_VuArray` rule, one layer up.
    var work = List[_Work](capacity=_THREADS)
    var threads = List[Int](length=_THREADS, fill=0)
    for t in range(_THREADS):
        work.append(
            _Work(
                Int(Pointer(to=producer)),
                Int(tokens.unsafe_ptr()),
                t * _PER_THREAD,
                _PER_THREAD,
            )
        )

    for t in range(_THREADS):
        # The element's own address rather than base + t * stride: the
        # stride of a *Mojo* struct is the compiler's business, and this
        # package hand-computes offsets only for C structs it has probed.
        var rc = create(
            Int(threads.unsafe_ptr()) + t * PTR_STRIDE,
            0,
            _produce_worker,
            Int(Pointer(to=work[t])),
        )
        assert_equal(Int(rc), 0, "pthread_create failed")

    # A concurrent *reader* while the workers produce and drain: this is the
    # side of the latch the worker threads do not exercise.
    var observed = 0
    for _ in range(200):
        observed = producer.delivery_failures()

    for t in range(_THREADS):
        _ = join(threads[t], 0)

    var raised = False
    try:
        producer.flush(20000)
    except:
        raised = True
    assert_true(raised, "flush() reported success for messages that timed out")

    # Every token distinct, none zero, none missing. A presence array says
    # all three at once: a duplicate trips the second mark, a lost increment
    # leaves a hole, and a thread that died leaves its slot at 0, which is
    # out of range because sequences start at 1.
    var seen = List[Bool](length=_TOTAL + 1, fill=False)
    for token in tokens:
        assert_true(
            token >= 1 and token <= _TOTAL,
            "token out of range: " + String(token),
        )
        assert_true(
            not seen[token], "two messages shared token " + String(token)
        )
        seen[token] = True
    for i in range(1, _TOTAL + 1):
        assert_true(seen[i], "no message ever claimed token " + String(i))

    var reports = producer.take_failures()
    assert_equal(
        len(reports), _TOTAL, "a delivery report was lost between threads"
    )
    assert_equal(len(producer.failures()), 0)
    assert_true(observed >= 0, "concurrent read of the failure count faulted")
    print(
        "    ",
        _THREADS,
        "threads produced",
        _TOTAL,
        "distinct tokens and",
        len(reports),
        "reports",
    )


# --- concurrent consumer teardown -------------------------------------------


@fieldwise_init
struct _CloseWork(Copyable, Movable):
    """One racing `close()` caller and the slot it reports into.

    `gate` is a start barrier. Without it the threads are staggered by the
    `pthread_create` loop itself, and an unsubscribed `close()` returns so
    fast that thread 1 is finished before thread 2 exists -- which is
    exactly why the first version of this test passed against the very race
    it was written to catch.
    """

    var consumer: Int
    var results: Int
    var index: Int
    var gate: Int


def _close_worker(arg: Int) abi("C") -> Int:
    """Call `close()` and record whether it reported an error.

    Slots are disjoint per thread, so nothing out here needs a lock: what is
    under test is inside `Consumer`.
    """
    ref work = Pointer[_CloseWork, ImmutAnyOrigin](unsafe_from_address=arg)[
        unsafe_offset=0
    ]
    ref consumer = Pointer[Consumer, MutAnyOrigin](
        unsafe_from_address=work.consumer
    )[unsafe_offset=0]
    var slots = Pointer[Int, MutAnyOrigin](unsafe_from_address=work.results)
    # Line up on the barrier so every thread calls close() at once.
    ref gate = Pointer[Atomic[DType.int64], MutAnyOrigin](
        unsafe_from_address=work.gate
    )[unsafe_offset=0]
    while gate.load() == 0:
        pass
    # `close()` is the only thing here that can raise, and an `abi("C")`
    # function may not -- so the verdict is recorded rather than propagated.
    try:
        consumer.close()
        slots[unsafe_offset=work.index] = 0
    except:
        slots[unsafe_offset=work.index] = 1
    return 0


def test_racing_close_calls_close_the_consumer_exactly_once() raises:
    """`close()` is documented safe to call more than once. That has to hold
    across threads too.

    It used to be a plain read of a `Bool` followed by a write, so every
    thread could pass the check before any of them set it. **Measured, and
    worse than it sounds:** with the compare-exchange reverted, all 8
    threads reach `rd_kafka_consumer_close`, exactly one returns, and the
    other 7 never come back -- concurrent closes of one consumer handle
    deadlock inside librdkafka. Sequentially the second close merely returns
    -197, which is what made this look like a cosmetic problem.

    So note the failure mode: if this regresses **the run hangs rather than
    fails**. That is the bug being caught, not a flaw in the test -- 3 runs
    out of 3 hang with the fix reverted, and 3 out of 3 pass in well under a
    second with it.

    No broker: nothing listens on port 9, and an unsubscribed consumer has
    no group to leave, so `close()` is local and prompt.
    """
    var libc = _open_libc()
    var create = libc.get_function[Int32]("pthread_create")
    var join = libc.get_function[Int32]("pthread_join")

    var cfg = ConsumerConfig(
        bootstrap_servers="127.0.0.1:9", group_id="race-close"
    )
    cfg.set("log_level", "0")
    var consumer = Consumer(cfg)

    var results = List[Int](length=_THREADS, fill=-1)
    var gate = Atomic[DType.int64](0)
    var work = List[_CloseWork](capacity=_THREADS)
    var threads = List[Int](length=_THREADS, fill=0)
    for t in range(_THREADS):
        work.append(
            _CloseWork(
                Int(Pointer(to=consumer)),
                Int(results.unsafe_ptr()),
                t,
                Int(Pointer(to=gate)),
            )
        )
    for t in range(_THREADS):
        var rc = create(
            Int(threads.unsafe_ptr()) + t * PTR_STRIDE,
            0,
            _close_worker,
            Int(Pointer(to=work[t])),
        )
        assert_equal(Int(rc), 0, "pthread_create failed")

    # Every thread is now spinning on the barrier; release them together.
    gate.store(1)

    for t in range(_THREADS):
        _ = join(threads[t], 0)

    # **Load-bearing.** The consumer's last use would otherwise be
    # `Pointer(to=consumer)` in the loop above, and Mojo destroys a value at
    # its last use -- so it would be torn down *before* the threads ran, and
    # every one of them would race on freed memory and return early. The
    # first version of this test did exactly that and passed against the
    # very bug it was written to catch.
    _ = consumer^

    var errors = 0
    for r in results:
        assert_true(r >= 0, "a close() thread never reported")
        errors += r
    assert_equal(
        errors,
        0,
        String(errors)
        + " of "
        + String(_THREADS)
        + " racing close() calls"
        " reported an error; only one may reach rd_kafka_consumer_close",
    )
    print("    ", _THREADS, "racing close() calls, none reported an error")


def test_consume_rejects_a_batch_size_it_cannot_honour() raises:
    """`n` is bounded at both ends, and the upper bound is the load-bearing one.

    The array of `rd_kafka_message_t*` is allocated **before** the call, so an
    unbounded `n` turns one caller's slip -- a variable holding a byte count
    rather than a message count, say -- into a multi-gigabyte allocation
    before a single record has arrived. `confluent-kafka` caps `num_messages`
    at 1M for the same reason and this matches it, checked against
    `confluent-kafka` 2.x rather than assumed.

    **0 is a deliberate divergence.** `confluent-kafka` accepts `consume(0)`
    and hands back an empty list; this raises. A drain loop built on a call
    that can silently return nothing for a reason unrelated to the topic
    spins forever, and 0 is only ever reached by accident.
    """
    var cfg = ConsumerConfig(
        bootstrap_servers="127.0.0.1:9", group_id="batch-bounds"
    )
    cfg.set("log_level", "0")
    var consumer = Consumer(cfg)

    for bad in [0, -1, MAX_BATCH + 1]:
        var raised = False
        try:
            _ = consumer.consume(bad, timeout_ms=10)
        except e:
            raised = True
            assert_true(
                String(e).find("n must be between") >= 0,
                "unexpected error for n=" + String(bad) + ": " + String(e),
            )
        assert_true(raised, "consume(" + String(bad) + ") was accepted")

    # The bound itself must be reachable -- an off-by-one here would reject
    # the largest legal batch and nothing would notice.
    _ = consumer.consume(MAX_BATCH, timeout_ms=10)
    consumer.close()
    print("    consume() bounds n to 1 ..", MAX_BATCH)


def test_consume_after_close_raises_instead_of_faulting() raises:
    """A closed consumer has no queue, and NULL is not one librdkafka checks.

    `close()` drops the consumer-queue reference **before**
    `rd_kafka_consumer_close`, which librdkafka requires -- so `_queue` is 0
    afterwards, and handing that to `rd_kafka_consume_batch_queue` faults
    inside `rd_kafka_consume_batch0` rather than returning an error. Measured:
    it segfaults 1 run in 1 without the guard.

    `poll()` after `close()` does **not** crash, so this asymmetry belongs to
    the batch path alone. Note the failure mode if this regresses: the run
    crashes rather than fails, the same shape as
    `test_racing_close_calls_close_the_consumer_exactly_once`.

    No broker: nothing listens on port 9, and an unsubscribed consumer has no
    group to leave, so `close()` is local and prompt.
    """
    var cfg = ConsumerConfig(
        bootstrap_servers="127.0.0.1:9", group_id="consume-after-close"
    )
    cfg.set("log_level", "0")
    var consumer = Consumer(cfg)
    consumer.close()

    var raised = False
    try:
        _ = consumer.consume(4, timeout_ms=200)
    except e:
        raised = True
        var text = String(e)
        assert_true(
            text.find("the consumer is closed") >= 0,
            "unexpected error: " + text,
        )
    assert_true(raised, "consume() after close() reported success")

    # The event form takes the same path and must be guarded by the same
    # check, not by a copy of it that could drift.
    raised = False
    try:
        _ = consumer.consume_events(4, timeout_ms=200)
    except:
        raised = True
    assert_true(raised, "consume_events() after close() reported success")
    print("    consume() after close() raised rather than faulting")


@fieldwise_init
struct _ConsumeWork(Copyable, Movable):
    """One racing `consume()` caller and the slot it reports into."""

    var consumer: Int
    var results: Int
    var index: Int
    var gate: Int


def _consume_worker(arg: Int) abi("C") -> Int:
    """Call `consume()` and record which of the three things happened.

    0 = returned normally, 1 = refused as a concurrent caller, 2 = some other
    error. `abi("C")` may not raise, so the verdict is recorded.
    """
    ref work = Pointer[_ConsumeWork, ImmutAnyOrigin](unsafe_from_address=arg)[
        unsafe_offset=0
    ]
    ref consumer = Pointer[Consumer, MutAnyOrigin](
        unsafe_from_address=work.consumer
    )[unsafe_offset=0]
    var slots = Pointer[Int, MutAnyOrigin](unsafe_from_address=work.results)
    ref gate = Pointer[Atomic[DType.int64], MutAnyOrigin](
        unsafe_from_address=work.gate
    )[unsafe_offset=0]
    while gate.load() == 0:
        pass
    try:
        _ = consumer.consume(16, timeout_ms=1500)
        slots[unsafe_offset=work.index] = 0
    except e:
        var text = String(e)
        if text.find("already inside consume()") >= 0:
            slots[unsafe_offset=work.index] = 1
        else:
            slots[unsafe_offset=work.index] = 2
    return 0


def test_concurrent_consume_is_refused_not_serialised() raises:
    """Two `consume()` calls on one consumer must not both run.

    librdkafka is explicit that concurrent `rd_kafka_consume_batch_queue` on
    one queue is **undefined behaviour** and that the case "will not be
    supported in future as well" -- and it is silent about the violation, so
    nothing detects it but us. It is also not in `rdkafka.h`, only in
    `INTRODUCTION.md`, so a reader of the header would never know.

    The rule could be honoured by serialising, and that would be wrong: it
    hides the caller's bug and silently changes what their program does.
    A second caller is **refused**.

    This is deterministic rather than a race to lose: the winner sits inside
    the batch call for its full 1.5s timeout against a dead port, so the
    other seven arrive while it is unambiguously still in there. Exactly one
    return and seven refusals is the assertion, and it fails both ways --
    serialise the latch instead of refusing, or drop it entirely, and all
    eight return.
    """
    var libc = _open_libc()
    var create = libc.get_function[Int32]("pthread_create")
    var join = libc.get_function[Int32]("pthread_join")

    var cfg = ConsumerConfig(
        bootstrap_servers="127.0.0.1:9", group_id="race-consume"
    )
    cfg.set("log_level", "0")
    var consumer = Consumer(cfg)

    var results = List[Int](length=_THREADS, fill=-1)
    var gate = Atomic[DType.int64](0)
    var work = List[_ConsumeWork](capacity=_THREADS)
    var threads = List[Int](length=_THREADS, fill=0)
    for t in range(_THREADS):
        work.append(
            _ConsumeWork(
                Int(Pointer(to=consumer)),
                Int(results.unsafe_ptr()),
                t,
                Int(Pointer(to=gate)),
            )
        )
    for t in range(_THREADS):
        var rc = create(
            Int(threads.unsafe_ptr()) + t * PTR_STRIDE,
            0,
            _consume_worker,
            Int(Pointer(to=work[t])),
        )
        assert_equal(Int(rc), 0, "pthread_create failed")

    gate.store(1)
    for t in range(_THREADS):
        _ = join(threads[t], 0)

    # Load-bearing, for the same reason as the racing-close case: without
    # this the consumer's last use is in the loop above and it is destroyed
    # before the threads ever run.
    _ = consumer^

    var returned = 0
    var refused = 0
    for r in results:
        assert_true(r >= 0, "a consume() thread never reported")
        if r == 0:
            returned += 1
        elif r == 1:
            refused += 1
        else:
            raise Error("a consume() thread failed for an unexpected reason")
    assert_equal(
        returned,
        1,
        String(returned)
        + " threads were inside consume() at once; exactly 1 may be",
    )
    assert_equal(refused, _THREADS - 1, "a concurrent caller was not refused")
    print(
        "    1 of", _THREADS, "consume() callers ran;", refused, "were refused"
    )


def test_close_waits_for_a_batch_fetch_in_flight() raises:
    """`close()` from another thread must not pull the consumer queue out
    from under a running batch fetch.

    librdkafka requires the queue reference to be destroyed before
    `rd_kafka_consumer_close`, and `rd_kafka_consume_batch_queue` reads
    through that reference for its whole timeout. `close()` used to release
    it on nothing more than the closed flag, so a fetch on another thread
    went on reading through a destroyed queue. Now the closed check runs
    under the batch latch and `close()` takes the same latch, so it waits
    for the fetch to return.

    Deterministic, like the concurrent-consume case: the fetch sits in the
    batch call for its full 1.5s against a dead port, `close()` is called
    300ms in, and must not return before the remaining ~1.2s has passed.
    An unsubscribed consumer's close is otherwise local and prompt, so a
    `close()` that took under 900ms did not wait.
    """
    var libc = _open_libc()
    var create = libc.get_function[Int32]("pthread_create")
    var join = libc.get_function[Int32]("pthread_join")

    var cfg = ConsumerConfig(
        bootstrap_servers="127.0.0.1:9", group_id="close-under-fetch"
    )
    cfg.set("log_level", "0")
    var consumer = Consumer(cfg)

    var results = List[Int](length=1, fill=-1)
    var gate = Atomic[DType.int64](1)
    var work = _ConsumeWork(
        Int(Pointer(to=consumer)),
        Int(results.unsafe_ptr()),
        0,
        Int(Pointer(to=gate)),
    )
    var thread = List[Int](length=1, fill=0)
    var rc = create(
        Int(thread.unsafe_ptr()), 0, _consume_worker, Int(Pointer(to=work))
    )
    assert_equal(Int(rc), 0, "pthread_create failed")

    sleep(0.3)
    var started = perf_counter_ns()
    consumer.close()
    var waited_ms = (perf_counter_ns() - started) // 1_000_000
    _ = join(thread[0], 0)
    # Pinned past the join: the thread reaches both through raw addresses.
    _ = work^
    _ = gate^

    assert_equal(results[0], 0, "the fetch did not return normally")
    assert_true(
        waited_ms >= 900,
        "close() returned after "
        + String(waited_ms)
        + "ms without waiting for the fetch in flight",
    )
    var raised = False
    try:
        _ = consumer.consume(16, timeout_ms=100)
    except:
        raised = True
    assert_true(raised, "consume() after the close reported success")
    print("    close() waited", waited_ms, "ms for the fetch in flight")


# --- observability -----------------------------------------------------------
#
# Errors, statistics and logs from librdkafka's background threads, retained
# on the client. Nothing listens on port 9, which is what makes the first
# two deterministic: connecting fails at once and librdkafka says so.


def test_a_dead_broker_is_reported_through_errors() raises:
    """A broker that is not there must show up in `errors()`.

    Before the error callback existed, a producer whose brokers had gone
    away found out only if a `flush()` happened to time out -- and a job
    that produced nothing for a while never found out at all. Connection
    refused is a `__TRANSPORT` error, which librdkafka reports through the
    callback once per distinct failure, and one poll is enough to serve it.
    """
    var cfg = ProducerConfig(bootstrap_servers="127.0.0.1:9")
    cfg.set("message.timeout.ms", "300")
    cfg.set("log_level", "0")
    var p = Producer(cfg)
    _ = p.produce(topic="nowhere", value="v")
    var deadline = perf_counter_ns() + 1_500_000_000
    while perf_counter_ns() < deadline and len(p.errors()) == 0:
        _ = p.poll(100)

    var seen = p.errors()
    assert_true(len(seen) > 0, "a dead broker produced no error")
    var transport = 0
    for err in seen:
        if err.kind() == KIND_TRANSPORT:
            transport += 1
        assert_true(
            err.message != "", "an error with no reason: " + String(err)
        )
    assert_true(
        transport > 0,
        "no KIND_TRANSPORT among " + String(len(seen)) + " errors",
    )
    assert_true(not p.fatal_error(), "a refused connection is not fatal")
    assert_equal(p.dropped_errors(), 0)

    # `take_errors()` acknowledges; `errors()` does not.
    var taken = p.take_errors()
    assert_equal(len(taken), len(seen))
    assert_equal(len(p.errors()), 0, "take_errors() did not clear")
    print("    dead broker:", seen[0])
    _ = p.take_failures()


def test_statistics_arrive_at_the_configured_interval() raises:
    """`statistics_interval_ms` on either config feeds `latest_stats()`.

    The document is emitted on a timer whether or not a broker answers, so
    a dead port is enough. Both clients are asserted on because they reach
    the callback through different queues -- the consumer's is forwarded by
    `poll_set_consumer` -- and a forwarding mistake would silence exactly
    one of them.
    """
    var ccfg = ConsumerConfig(
        bootstrap_servers="127.0.0.1:9",
        group_id="stats",
        statistics_interval_ms=100,
    )
    ccfg.set("log_level", "0")
    var consumer = Consumer(ccfg)
    assert_true(not consumer.latest_stats(), "stats before the first tick")
    var deadline = perf_counter_ns() + 2_000_000_000
    while perf_counter_ns() < deadline and not consumer.latest_stats():
        _ = consumer.poll(100)
    var stats = consumer.latest_stats()
    assert_true(Bool(stats), "no statistics document arrived on the consumer")
    assert_true(
        '"name"' in stats.value(),
        "not a statistics document: " + stats.value()[byte=0:80],
    )
    assert_true(
        '"type": "consumer"' in stats.value(),
        "the consumer's document does not say it is one",
    )
    consumer.close()

    var pcfg = ProducerConfig(
        bootstrap_servers="127.0.0.1:9", statistics_interval_ms=100
    )
    pcfg.set("log_level", "0")
    var producer = Producer(pcfg)
    deadline = perf_counter_ns() + 2_000_000_000
    while perf_counter_ns() < deadline and not producer.latest_stats():
        _ = producer.poll(100)
    var pstats = producer.latest_stats()
    assert_true(Bool(pstats), "no statistics document arrived on the producer")
    assert_true(
        '"type": "producer"' in pstats.value(),
        "the producer's document does not say it is one",
    )
    print(
        "    statistics:",
        stats.value().byte_length(),
        "bytes for the consumer",
    )


def test_captured_logs_carry_a_facility() raises:
    """`capture_logs=True` retains librdkafka's log lines; off, nothing is.

    `log_level=7` turns on debug lines so a dead port generates plenty
    within a few hundred milliseconds. Every line librdkafka writes has a
    facility -- `CONNECT`, `FAIL`, `BROKERFAIL` -- so an empty one means
    the C string was decoded from the wrong argument.

    The off case matters as much: the log hook forces `log.queue=true`, so
    a consumer that did not ask must not have it installed.
    """
    var cfg = ConsumerConfig(
        bootstrap_servers="127.0.0.1:9", group_id="logs", capture_logs=True
    )
    cfg.set("log_level", "7")
    var consumer = Consumer(cfg)
    var deadline = perf_counter_ns() + 2_000_000_000
    while perf_counter_ns() < deadline and len(consumer.logs()) == 0:
        _ = consumer.poll(100)
    var lines = consumer.logs()
    assert_true(len(lines) > 0, "no log lines were captured")
    for line in lines:
        assert_true(
            line.facility != "", "a log line with no facility: " + String(line)
        )
        assert_true(line.message != "", "a log line with no text")
        assert_true(
            line.level >= 0 and line.level <= 7,
            "not a syslog level: " + String(line.level),
        )
    assert_true(len(lines) <= RETAINED, "logs() exceeded its bound")
    var taken = consumer.take_logs()
    assert_true(len(taken) >= len(lines), "take_logs() returned fewer")
    assert_equal(len(consumer.logs()), 0, "take_logs() did not clear")
    consumer.close()

    var quiet = ConsumerConfig(bootstrap_servers="127.0.0.1:9", group_id="q")
    quiet.set("log_level", "0")
    var silent = Consumer(quiet)
    _ = silent.poll(200)
    assert_equal(len(silent.logs()), 0, "logs captured without capture_logs")
    silent.close()
    print("    captured", len(lines), "log lines; first:", lines[0])


def test_retained_errors_are_bounded_and_the_rest_counted() raises:
    """A flapping broker must not grow `errors()` without bound.

    A thousand errors are driven straight through the C trampoline -- the
    same function librdkafka would call, with the producer's own opaque --
    because a dead port yields one transport error per reconnect and a real
    burst would take minutes. The producer is never polled here, so nothing
    but the burst reaches the list and the counts are exact.

    Asserts on which entries survive, not just how many: the bound has to
    drop the *oldest*, or a job reading `errors()` after a storm sees the
    first minute of it and not the last.
    """
    var cfg = ProducerConfig(bootstrap_servers="127.0.0.1:9")
    cfg.set("log_level", "0")
    var p = Producer(cfg)
    var opaque = Int(p._dr.unsafe_ptr())

    var burst = 1000
    for i in range(burst):
        var reason = _c_string("burst-" + String(i))
        _error_trampoline[_DrState](
            0, RD_KAFKA_RESP_ERR__TRANSPORT, Int(reason.unsafe_ptr()), opaque
        )
        _ = reason^

    var kept = p.errors()
    assert_equal(len(kept), RETAINED, "errors() is not bounded at 256")
    assert_equal(p.dropped_errors(), burst - RETAINED)
    assert_equal(kept[0].message, "burst-" + String(burst - RETAINED))
    assert_equal(kept[RETAINED - 1].message, "burst-" + String(burst - 1))
    assert_true(kept[0].kind() == KIND_TRANSPORT)

    _ = p.take_errors()
    assert_equal(len(p.errors()), 0)
    # The counter is cumulative: acknowledging does not reset it.
    assert_equal(p.dropped_errors(), burst - RETAINED)
    print(
        "    kept",
        RETAINED,
        "of",
        burst,
        "errors, dropped",
        p.dropped_errors(),
    )


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
