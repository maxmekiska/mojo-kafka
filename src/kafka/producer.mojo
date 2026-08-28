"""High-level producer."""

from std.time import perf_counter_ns

from ._ffi import (
    Lib,
    MSG_ERR,
    MSG_RKT,
    RD_KAFKA_EVENT_DR,
    RD_KAFKA_PARTITION_UA,
    RD_KAFKA_PRODUCER,
    RD_KAFKA_RESP_ERR_NO_ERROR,
    RD_KAFKA_RESP_ERR__TIMED_OUT,
    _load_i32,
    _load_word,
)
from .config import ProducerConfig


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
    """

    var _lib: Lib
    var _rk: Int
    var _dr_queue: Int
    var _topics: Dict[String, Int]
    var _failed: Int
    var _first_failure: String

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
        self._topics = Dict[String, Int]()
        self._failed = 0
        self._first_failure = String("")

    def __deinit__(deinit self):
        # Destructors cannot raise, and there is nothing useful to do with a
        # teardown failure anyway -- the process is already letting go.
        if self._rk != 0:
            try:
                _ = self._drain_until_empty(5000)
                for entry in self._topics.items():
                    self._lib.topic_destroy(entry.value)
                if self._dr_queue != 0:
                    self._lib.queue_destroy(self._dr_queue)
                self._lib.destroy(self._rk)
            except:
                pass

    def _topic_handle(mut self, topic: String) raises -> Int:
        """Topic handles are per-name and reusable, so cache them."""
        if topic in self._topics:
            return self._topics[topic]
        var rkt = self._lib.topic_new(self._rk, topic)
        self._topics[topic] = rkt
        return rkt

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
                        self._failed += 1
                        if self._first_failure == "":
                            self._first_failure = (
                                self._lib.topic_name(_load_word(m + MSG_RKT))
                                + ": "
                                + self._lib.error(err).describe()
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

    def _raise_if_undelivered(mut self) raises:
        """Report and clear rejections seen since the last check."""
        if self._failed == 0:
            return
        var n = self._failed
        var first = self._first_failure
        self._failed = 0
        self._first_failure = String("")
        raise Error(
            String(n) + " message(s) failed delivery; first was " + first
        )

    def delivery_failures(self) -> Int:
        """Rejections tallied since the last `flush()`, without blocking."""
        return self._failed

    # -- producing ------------------------------------------------------------

    def _enqueue(
        mut self,
        topic: String,
        payload: Int,
        payload_len: Int,
        key: Int,
        key_len: Int,
    ) raises:
        var rkt = self._topic_handle(topic)
        var rc = self._lib.produce(
            rkt, RD_KAFKA_PARTITION_UA, payload, payload_len, key, key_len
        )
        if rc == -1:
            raise Error(
                "produce("
                + topic
                + "): "
                + self._lib.error(self._lib.last_error()).describe()
            )

    def produce(
        mut self,
        topic: String,
        value: String,
        key: String = "",
    ) raises:
        """Enqueue a message.

        Returns as soon as the message is queued; it is handed to the broker
        in the background, and the broker's verdict surfaces at `flush()`.
        librdkafka copies the payload, so `value` and `key` can go out of
        scope immediately.
        """
        var key_ptr = 0
        var key_len = 0
        if key.byte_length() > 0:
            key_ptr = Int(key.unsafe_ptr())
            key_len = key.byte_length()
        self._enqueue(
            topic,
            Int(value.unsafe_ptr()),
            value.byte_length(),
            key_ptr,
            key_len,
        )

    def produce_bytes(
        mut self,
        topic: String,
        value: List[UInt8],
        key: List[UInt8] = List[UInt8](),
    ) raises:
        """Enqueue an arbitrary byte payload -- Avro, Protobuf, a compressed blob.

        `produce()` takes a `String`, which cannot carry non-UTF-8 bytes, so
        this is how binary is written. The consume side is still `String`
        (see `Message`); its bytes survive intact via `as_bytes()`.
        """
        var key_ptr = 0
        var key_len = 0
        if len(key) > 0:
            key_ptr = Int(key.unsafe_ptr())
            key_len = len(key)
        self._enqueue(
            topic, Int(value.unsafe_ptr()), len(value), key_ptr, key_len
        )

    def poll(mut self, timeout_ms: Int32 = 0) raises -> Int32:
        """Serve delivery reports. Returns how many were collected.

        Rejections are tallied as they arrive; `flush()` raises on them and
        `delivery_failures()` reads the running count.
        """
        return Int32(self._drain(timeout_ms))

    def flush(mut self, timeout_ms: Int32 = 5000) raises:
        """Block until every queued message is acknowledged.

        Raises if the queue does not drain in time, **and** if any message
        was rejected -- an undelivered message is never reported as success.
        """
        if not self._drain_until_empty(timeout_ms):
            var remaining = self._lib.outq_len(self._rk)
            raise Error(
                "flush: "
                + self._lib.error(RD_KAFKA_RESP_ERR__TIMED_OUT).describe()
                + " ("
                + String(Int(remaining))
                + " message(s) still queued)"
            )
        self._raise_if_undelivered()
