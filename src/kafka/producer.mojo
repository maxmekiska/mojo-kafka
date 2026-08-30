"""High-level producer."""

from std.time import perf_counter_ns

from ._ffi import (
    Lib,
    MSG_ERR,
    MSG_OFFSET,
    MSG_PARTITION,
    MSG_PRIVATE,
    MSG_RKT,
    RD_KAFKA_EVENT_DR,
    RD_KAFKA_MSG_F_COPY,
    RD_KAFKA_PARTITION_UA,
    RD_KAFKA_PRODUCER,
    RD_KAFKA_RESP_ERR_NO_ERROR,
    RD_KAFKA_RESP_ERR__TIMED_OUT,
    KIND_OTHER,
    KafkaErrorKind,
    kind_of,
    _c_string,
    _load_i32,
    _load_i64,
    _load_word,
    _VuArray,
)
from .config import ProducerConfig
from .header import Header

# Let librdkafka choose the partition with the topic's partitioner, which is
# what `produce()` does unless a caller names one. Re-exported under a less
# cryptic name than librdkafka's "UA" (unassigned).
comptime PARTITION_UNASSIGNED: Int32 = RD_KAFKA_PARTITION_UA


@fieldwise_init
struct DeliveryReport(Copyable, Movable, Writable):
    """What the broker said about one produced message.

    `sequence` is the token `produce()` returned, which is what makes a
    verdict addressable: without it a rejection is just a tally and the text
    of whichever failure happened to arrive first.

    Only **failures** are retained -- see `Producer.failures()` for why -- so
    an instance of this always describes a message that did not make it.
    `partition` and `offset` are whatever librdkafka filled in, and for a
    message that never reached the broker they are its unassigned defaults.
    """

    var sequence: Int
    var topic: String
    var partition: Int32
    var offset: Int64
    var error_code: Int32
    var error: String

    def kind(self) -> KafkaErrorKind:
        """The branchable category of this rejection.

        `error_code` keeps the exact librdkafka value; this is the one to
        branch on. A `KIND_TIMED_OUT` report is worth retrying, a
        `KIND_MESSAGE_TOO_LARGE` one never is.
        """
        return kind_of(self.error_code)

    def write_to(self, mut writer: Some[Writer]):
        """One line, naming the message by the token `produce()` returned.

        `Writable` rather than a bespoke `describe()`, so `String(report)`
        and `print(report)` behave like any other Mojo value.
        """
        writer.write(
            "#",
            self.sequence,
            " ",
            self.topic,
            "[",
            self.partition,
            "]: ",
            self.error,
        )


struct _Field(Copyable, Movable):
    """One key or value on its way to C: where it is, how long, and whether
    it is there at all.

    Presence is tracked separately from length because Kafka needs all three
    states -- absent, present-and-empty, present-with-bytes -- and a length
    alone collapses the first two. It exists to describe a buffer the caller
    still owns; it must not outlive the `produce()` call it was built for.

    Deliberately **not** `@fieldwise_init`: that would synthesise
    `_Field(pointer, length, present)`, restoring the bare `(Int, Int, Bool)`
    call this type exists to remove. The only way in is from an `Optional`,
    which is the one place the presence rule can be got right.
    """

    var pointer: Int
    var length: Int
    var present: Bool

    def __init__(out self, field: Optional[String]):
        """Encode text. `byte_length()` because `len()` on a String is UTF-8
        codepoints, and Kafka counts bytes."""
        self.present = field.__bool__()
        self.pointer = Int(field.value().unsafe_ptr()) if self.present else 0
        self.length = field.value().byte_length() if self.present else 0

    def __init__(out self, field: Optional[List[UInt8]]):
        """Encode raw bytes."""
        self.present = field.__bool__()
        self.pointer = Int(field.value().unsafe_ptr()) if self.present else 0
        self.length = len(field.value()) if self.present else 0

    def address(self, placeholder: Int) -> Int:
        """The address C should see: 0 when absent, never 0 when present."""
        if not self.present:
            return 0
        return self.pointer if self.pointer != 0 else placeholder


struct Producer:
    """A producer over librdkafka.

    Construct from a `ProducerConfig` and call `produce(...)` repeatedly.
    `flush()` before drop to wait for in-flight messages to be acked.

    **Delivery is verified, not assumed.** `produce()` only enqueues; the
    broker's verdict arrives later. The producer asks librdkafka for delivery
    reports as events on its main queue (`RD_KAFKA_EVENT_DR`), drains them in
    `poll()` and `flush()`, and `flush()` raises if any message was rejected.
    Without that, a message dropped at `message.timeout.ms` leaves the queue
    empty and a plain `rd_kafka_flush` reports success over the top of it.

    Messages go out through `rd_kafka_produceva`, which names the topic by
    string. That is what lets this hold no per-topic state: the producer used
    to cache an `rd_kafka_topic_t` per topic name, and that unsynchronised
    `Dict` was the one thing making it unsafe to share across threads when
    librdkafka's own handle is not.
    """

    var _lib: Lib
    var _rk: Int
    var _dr_queue: Int
    var _failures: List[DeliveryReport]
    var _next_sequence: Int
    var _last_error_kind: KafkaErrorKind

    def __init__(out self, cfg: ProducerConfig) raises:
        self._lib = Lib()
        var conf = cfg._build(self._lib)
        try:
            # Delivery reports as events, not callbacks: Mojo cannot hand C a
            # function pointer, and event sourcing does not need one.
            self._lib.conf_set_events(conf, RD_KAFKA_EVENT_DR)
        except e:
            self._lib.conf_destroy(conf)
            raise e
        # rd_kafka_new adopts conf on success and _build/new_client
        # between them free it on every failure path.
        self._rk = self._lib.new_client(RD_KAFKA_PRODUCER, conf)
        self._dr_queue = self._lib.queue_get_main(self._rk)
        self._failures = List[DeliveryReport]()
        # Sequences start at 1, not 0. The opaque travels as a `void *` and
        # comes back as `_private`, where 0 is indistinguishable from a
        # message produced without one.
        self._next_sequence = 1
        self._last_error_kind = KIND_OTHER

    def __deinit__(deinit self):
        # Destructors cannot raise, and there is nothing useful to do with a
        # teardown failure anyway -- the process is already letting go.
        #
        # The drain below is a **blocking 5s worst case**: dropping a producer
        # that still holds undeliverable messages waits for them to time out,
        # and their failures are then swallowed rather than raised. Call
        # `flush()` before dropping to see the verdict and to choose the wait.
        # The alternative -- a shorter deadline here -- would silently discard
        # messages that were still in flight, which is the worse default for a
        # client whose whole point is that delivery is verified.
        if self._rk != 0:
            try:
                _ = self._drain_until_empty(5000)
                if self._dr_queue != 0:
                    self._lib.queue_destroy(self._dr_queue)
                self._lib.destroy(self._rk)
            except:
                pass

    # -- delivery reports -----------------------------------------------------

    def _drain(mut self, timeout_ms: Int32) raises -> Int:
        """Consume one delivery-report batch, recording any rejections."""
        var ev = self._lib.queue_poll(self._dr_queue, timeout_ms)
        if ev == 0:
            return 0
        var seen = 0
        try:
            if self._lib.event_type(ev) == RD_KAFKA_EVENT_DR:
                for _ in range(self._lib.event_message_count(ev)):
                    var m = self._lib.event_message_next(ev)
                    if m == 0:
                        break
                    seen += 1
                    var err = _load_i32(m + MSG_ERR)
                    if err != RD_KAFKA_RESP_ERR_NO_ERROR:
                        # `_private` is the token handed to produceva as
                        # RD_KAFKA_VTYPE_OPAQUE, returned untouched.
                        self._failures.append(
                            DeliveryReport(
                                _load_word(m + MSG_PRIVATE),
                                self._lib.topic_name(_load_word(m + MSG_RKT)),
                                _load_i32(m + MSG_PARTITION),
                                _load_i64(m + MSG_OFFSET),
                                err,
                                String(self._lib.error(err)),
                            )
                        )
        except e:
            self._lib.event_destroy(ev)
            raise e
        self._lib.event_destroy(ev)
        return seen

    def _drain_until_empty(mut self, timeout_ms: Int32) raises -> Bool:
        """Serve delivery reports until nothing is outstanding.

        `rd_kafka_flush` is deliberately not used: with `RD_KAFKA_EVENT_DR`
        enabled it expects a second thread to be serving the queue, and
        `rd_kafka_outq_len` counts undrained events as outstanding, so it
        would sit there until the timeout.
        """
        var deadline = Int(perf_counter_ns()) + Int(timeout_ms) * 1_000_000
        while True:
            if self._lib.outq_len(self._rk) == 0:
                return True
            _ = self._drain(50)
            if Int(perf_counter_ns()) >= deadline:
                return self._lib.outq_len(self._rk) == 0

    def _raise_if_undelivered(self) raises:
        """Raise if any rejection is still unacknowledged.

        Deliberately does **not** clear: the reports are the useful part, and
        discarding them as the error is raised would leave the caller with a
        count and a string -- exactly what per-message reports exist to
        replace. `take_failures()` is how they are acknowledged.
        """
        if len(self._failures) == 0:
            return
        raise Error(
            String(len(self._failures))
            + " message(s) failed delivery; first was "
            + String(self._failures[0])
            + " (call take_failures() for all of them)"
        )

    def last_error_kind(self) -> KafkaErrorKind:
        """Why the most recent `produce()` was rejected, as a branchable kind.

        `produce()` raises, and a Mojo `Error` carries only text, so the kind
        is read here instead of off the exception. The case this exists for is
        backpressure -- `confluent-kafka-python` raises `BufferError` for it
        precisely so callers can drain and retry:

            try:
                _ = p.produce("t", "payload")
            except e:
                if p.last_error_kind() == KIND_QUEUE_FULL:
                    _ = p.poll(100)      # serve reports, free queue space
                else:
                    raise e

        Only meaningful immediately after a `produce()` that raised.
        """
        return self._last_error_kind

    def delivery_failures(self) -> Int:
        """Rejections tallied since the last `flush()`, without blocking."""
        return len(self._failures)

    def failures(self) -> List[DeliveryReport]:
        """Every unacknowledged rejection, each naming the sequence
        `produce()` returned for it. Does not acknowledge them.

        Successful deliveries are deliberately **not** retained. A report per
        message would grow without bound in a long-running producer that
        never reads them, and the question worth answering -- which messages
        did not make it -- needs only the failures. After a `flush()` that
        does not raise, every message produced before it was delivered.
        """
        return self._failures.copy()

    def take_failures(mut self) -> List[DeliveryReport]:
        """Acknowledge every rejection, returning what was outstanding.

        `flush()` keeps raising while any rejection is unacknowledged, so
        this is how a caller says it has handled them:

            try:
                p.flush()
            except e:
                for report in p.take_failures():
                    print(report)
        """
        var taken = self._failures.copy()
        self._failures.clear()
        return taken^

    # -- producing ------------------------------------------------------------

    def _headers_handle(self, headers: List[Header]) raises -> Int:
        """Build a `rd_kafka_headers_t` from `headers`, or 0 if there are none.

        On success the list is handed to `produceva`, which adopts it -- so
        the caller must destroy it only if the produce call never happens or
        comes back with an error. A half-built list is destroyed here.

        Header names are passed as pointer + byte length rather than as C
        strings: `String.unsafe_ptr()` is not NUL-terminated, and an explicit
        length keeps the name off that trap entirely. Header *values* follow
        the same pointer-presence rule as keys and payloads, so they reuse
        `_Field` and its placeholder.
        """
        if len(headers) == 0:
            return 0

        var hdrs = self._lib.headers_new(len(headers))
        if hdrs == 0:
            raise Error("rd_kafka_headers_new returned NULL")

        var placeholder = Array[UInt8, 1](fill=0)
        var somewhere = Int(placeholder.unsafe_ptr())
        try:
            for header in headers:
                var value = _Field(header.value)
                var rc = self._lib.header_add(
                    hdrs,
                    Int(header.name.unsafe_ptr()),
                    header.name.byte_length(),
                    value.address(somewhere),
                    value.length,
                )
                self._lib.raise_if(rc, "header_add(" + header.name + ")")
        except e:
            self._lib.headers_destroy(hdrs)
            raise e
        _ = placeholder^
        return hdrs

    def _enqueue(
        mut self,
        topic: String,
        value: _Field,
        key: _Field,
        headers: List[Header],
        partition: Int32,
    ) raises -> Int:
        """Hand one message to librdkafka, null fields and all.

        librdkafka reads a field's **presence from the pointer**, not from
        the length: NULL is a null field, and a non-NULL pointer with length
        0 is a field that is present and empty. That single rule is what
        makes tombstones (null payload) and empty-but-present keys
        expressible, so an absent field must reach C as address 0 and a
        present one must never do.

        An empty Mojo buffer does have a real address, but nothing in the
        language promises it is non-NULL, so a present field is pinned to
        `placeholder` if its own address is 0. Reading zero bytes from it is
        what librdkafka does with it either way.

        Every address in the `vu` array points at a buffer this frame owns,
        and Mojo releases a value after its last *use* rather than at the end
        of the scope -- hence the `_ = x^` line after the call, which is what
        keeps the topic name and the placeholder alive across it.
        """
        # Claimed before anything can fail, so a rejected message still has a
        # token the caller can match its report against.
        var sequence = self._next_sequence
        self._next_sequence += 1

        # Owned from here on: `produceva` adopts the header list only if it
        # succeeds, so every other exit has to destroy it.
        var hdrs = self._headers_handle(headers)

        var err: Int
        try:
            var placeholder = Array[UInt8, 1](fill=0)
            var somewhere = Int(placeholder.unsafe_ptr())
            var topic_c = _c_string(topic)

            # Seven entries, matching the calls below. `_entry` raises
            # rather than overrunning if this is ever left behind.
            var vus = _VuArray(7)
            vus.topic(Int(topic_c.unsafe_ptr()))
            vus.partition(partition)
            vus.msgflags(RD_KAFKA_MSG_F_COPY)
            vus.value(value.address(somewhere), value.length)
            vus.key(key.address(somewhere), key.length)
            vus.opaque(sequence)
            if hdrs != 0:
                vus.headers(hdrs)

            err = self._lib.produceva(self._rk, vus.address(), vus.count())
            _ = placeholder^
            _ = topic_c^
            _ = vus^
        except e:
            # Nothing reached librdkafka, so the headers are still ours.
            if hdrs != 0:
                self._lib.headers_destroy(hdrs)
            raise e

        # NULL is success here -- the opposite polarity to the handle-returning
        # calls elsewhere in this package.
        if err == 0:
            return sequence
        if hdrs != 0:
            self._lib.headers_destroy(hdrs)
        var failure = self._lib.take_error(err)
        # Recorded because the raise cannot carry it: Mojo 1.0's `Error` is
        # text, so `except` has no type to match on. See `last_error_kind`.
        self._last_error_kind = failure.kind()
        raise Error("produce(" + topic + "): " + String(failure))

    def produce(
        mut self,
        topic: String,
        value: Optional[String],
        key: Optional[String] = None,
        headers: List[Header] = [],
        partition: Int32 = PARTITION_UNASSIGNED,
    ) raises -> Int:
        """Enqueue a message whose key and value are text.

        Returns as soon as the message is queued; it is handed to the broker
        in the background, and the broker's verdict surfaces at `flush()`.
        librdkafka copies the payload, so `value` and `key` can go out of
        scope immediately.

        Both halves are `Optional` because Kafka's are. `None` writes a
        **null** field, `""` writes one that is present and empty, and the
        two are different messages on the wire:

            p.produce("t", "hello")                # null key
            p.produce("t", "hello", key="")        # empty key, present
            p.produce("t", None, key="k")          # tombstone

        `headers` is a list of pairs rather than a map, because Kafka lets a
        name repeat and keeps the order:

            p.produce("t", "hi", headers=[
                Header("content-type", "text/plain"),
                Header("trace-id", "abc123"),
            ])

        `partition` defaults to `PARTITION_UNASSIGNED`, which leaves the
        choice to the topic's partitioner -- key hashing, normally. Name one
        to bypass it.

        Returns a **sequence token** identifying this message. It comes back
        on the message's delivery report, so `failures()` can name exactly
        which messages the broker rejected. Ignore it if a count is enough.

        Use `produce_bytes()` for anything that is not valid UTF-8.
        """
        return self._enqueue(
            topic, _Field(value), _Field(key), headers, partition
        )

    def produce_bytes(
        mut self,
        topic: String,
        value: Optional[List[UInt8]],
        key: Optional[List[UInt8]] = None,
        headers: List[Header] = [],
        partition: Int32 = PARTITION_UNASSIGNED,
    ) raises -> Int:
        """Enqueue an arbitrary byte payload -- Avro, Protobuf, a compressed blob.

        The same null / empty / present rules as `produce()`, over bytes
        rather than text, and the same `headers` and `partition`. Mojo will
        not copy a `List` implicitly, so pass an owned one (`value^`) or an
        explicit `value.copy()`.

        Returns the same sequence token as `produce()`.
        """
        return self._enqueue(
            topic, _Field(value), _Field(key), headers, partition
        )

    def poll(mut self, timeout_ms: Int32 = 0) raises -> Int32:
        """Serve delivery reports. Returns how many were collected.

        Rejections are tallied as they arrive; `flush()` raises on them and
        `delivery_failures()` reads the running count.
        """
        return Int32(self._drain(timeout_ms))

    def flush(mut self, timeout_ms: Int32 = 5000) raises:
        """Block until every queued message is acknowledged.

        Raises if the queue does not drain in time, **and** while any message
        rejection is unacknowledged -- an undelivered message is never
        reported as success. Call `take_failures()` to acknowledge them and
        to see which messages they were.
        """
        if not self._drain_until_empty(timeout_ms):
            var remaining = self._lib.outq_len(self._rk)
            raise Error(
                "flush: "
                + String(self._lib.error(RD_KAFKA_RESP_ERR__TIMED_OUT))
                + " ("
                + String(Int(remaining))
                + " message(s) still queued)"
            )
        self._raise_if_undelivered()
