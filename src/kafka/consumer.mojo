"""High-level consumer."""

from ._ffi import (
    Lib,
    MSG_ERR,
    MSG_KEY,
    MSG_KEY_LEN,
    MSG_LEN,
    MSG_OFFSET,
    MSG_PARTITION,
    MSG_PAYLOAD,
    MSG_RKT,
    RD_KAFKA_CONSUMER,
    RD_KAFKA_RESP_ERR_NO_ERROR,
    RD_KAFKA_RESP_ERR__NOENT,
    RD_KAFKA_RESP_ERR__PARTITION_EOF,
    _load_i32,
    _load_i64,
    _load_word,
    copy_bytes,
    cstr,
)
from .config import ConsumerConfig
from .header import Header, _text


@fieldwise_init
struct Message(Copyable, Movable):
    """One consumed record.

    `key` and `value` are **optional byte arrays**, not strings, because that
    is what Kafka records actually are:

    - Kafka fields are opaque bytes. A `String` cannot hold arbitrary bytes,
      and any text decoding belongs to the caller, who knows the schema.
    - Either half may be **absent**, which Kafka distinguishes from present
      and empty. `None` here means the field was null on the wire; a present
      but empty field is `Some` wrapping an empty list. Conflating the two
      makes compaction tombstones -- non-null key, null value -- invisible.

    `key_text()` / `value_text()` decode for the common case where the
    payload really is text. They collapse null onto their `default`, so read
    `key` / `value` directly when the difference matters.

    `headers` is a **list of pairs, not a map**: Kafka permits a repeated
    header name and preserves order, both of which a `Dict` would quietly
    discard. It is empty for a record written without headers -- which is
    indistinguishable, on the wire, from one written with none.
    """

    var topic: String
    var partition: Int32
    var offset: Int64
    var key: Optional[List[UInt8]]
    var value: Optional[List[UInt8]]
    var headers: List[Header]

    def key_text(self, default: String = "") -> String:
        """The key decoded as UTF-8, or `default` if the key was null."""
        return _text(self.key, default)

    def value_text(self, default: String = "") -> String:
        """The value decoded as UTF-8, or `default` if the value was null."""
        return _text(self.value, default)

    def is_tombstone(self) -> Bool:
        """True for a compaction tombstone: a null value, key or not."""
        return not self.value

    def _index_of(self, name: String) -> Int:
        """Position of the first header called `name`, or -1.

        First rather than last because that is the order they were written
        in, and Kafka permits the name to repeat.
        """
        for i in range(len(self.headers)):
            if self.headers[i].name == name:
                return i
        return -1

    def header(self, name: String) -> Optional[List[UInt8]]:
        """The value of the **first** header called `name`, if there is one.

        Returns `None` both when no such header exists and when one exists
        with a null value; the two are different records, so walk `headers`
        directly if that matters.
        """
        var i = self._index_of(name)
        if i < 0:
            return None
        return self.headers[i].value.copy()

    def header_text(self, name: String, default: String = "") -> String:
        """The first `name` header decoded as UTF-8, or `default` if absent.

        Reads the value in place rather than going through `header()`, which
        would copy the bytes only for the decode to throw the copy away.
        """
        var i = self._index_of(name)
        if i < 0:
            return default
        return _text(self.headers[i].value, default)


struct Consumer:
    """A consumer group member over librdkafka."""

    var _lib: Lib
    var _rk: Int
    var _closed: Bool

    def __init__(out self, cfg: ConsumerConfig) raises:
        self._lib = Lib()
        var conf = cfg._build(self._lib)
        self._rk = self._lib.new_client(RD_KAFKA_CONSUMER, conf)
        _ = self._lib.poll_set_consumer(self._rk)
        self._closed = False

    def __deinit__(deinit self):
        # Destructors cannot raise; a failed close is not actionable here.
        if self._rk != 0:
            try:
                if not self._closed:
                    _ = self._lib.consumer_close(self._rk)
                self._lib.destroy(self._rk)
            except:
                pass

    def subscribe(self, topics: List[String]) raises:
        # Same shape as `poll` below: the list is owned here, and both the
        # adds and the subscribe can raise.
        var list = self._lib.topic_partition_list_new(Int32(len(topics)))
        var rc: Int32
        try:
            for topic in topics:
                _ = self._lib.topic_partition_list_add(list, topic, -1)
            rc = self._lib.subscribe(self._rk, list)
        except e:
            self._lib.topic_partition_list_destroy(list)
            raise e
        self._lib.topic_partition_list_destroy(list)
        self._lib.raise_if(rc, "subscribe")

    def poll(self, timeout_ms: Int32 = 1000) raises -> Optional[Message]:
        """Fetch the next message.

        Returns `None` on timeout and at end-of-partition -- neither is a
        message. Any other broker error is raised.
        """
        var raw = self._lib.consumer_poll(self._rk, timeout_ms)
        if raw == 0:
            return None

        var err_code = _load_i32(raw + MSG_ERR)
        if err_code == RD_KAFKA_RESP_ERR__PARTITION_EOF:
            self._lib.message_destroy(raw)
            return None
        if err_code != RD_KAFKA_RESP_ERR_NO_ERROR:
            var e = self._lib.error(err_code)
            self._lib.message_destroy(raw)
            raise Error("poll: " + String(e))

        # Every exit from here on has to destroy `raw`, including the ones
        # taken by a raising decode.
        var msg: Message
        try:
            msg = Message(
                self._lib.topic_name(_load_word(raw + MSG_RKT)),
                _load_i32(raw + MSG_PARTITION),
                _load_i64(raw + MSG_OFFSET),
                copy_bytes(
                    _load_word(raw + MSG_KEY), _load_word(raw + MSG_KEY_LEN)
                ),
                copy_bytes(
                    _load_word(raw + MSG_PAYLOAD), _load_word(raw + MSG_LEN)
                ),
                self._headers_of(raw),
            )
        except e:
            self._lib.message_destroy(raw)
            raise e
        self._lib.message_destroy(raw)
        return msg^

    def _headers_of(self, raw: Int) raises -> List[Header]:
        """Copy a raw message's headers out before the message is destroyed.

        The list `rd_kafka_message_headers` hands back is **borrowed**: it
        belongs to the message and dies with it, so every name and value is
        copied here rather than pointed at. Destroying it would double-free.

        A record with no headers is not an error -- librdkafka reports
        `__NOENT` for it, and so does the iterator once it runs off the end
        of the list. Both mean "stop", not "fail".
        """
        var out = List[Header]()
        var hdrs_out = Array[Int, 1](fill=0)
        var rc = self._lib.message_headers(raw, Int(hdrs_out.unsafe_ptr()))
        if rc == RD_KAFKA_RESP_ERR__NOENT:
            return out^
        self._lib.raise_if(rc, "message_headers")

        var hdrs = hdrs_out[0]
        if hdrs == 0:
            return out^

        # Reused across iterations: `header_get_all` overwrites all three on
        # every call, so rebuilding them per header is pure churn.
        var namep = Array[Int, 1](fill=0)
        var valuep = Array[Int, 1](fill=0)
        var sizep = Array[Int, 1](fill=0)
        var idx = 0
        while True:
            var got = self._lib.header_get_all(
                hdrs,
                idx,
                Int(namep.unsafe_ptr()),
                Int(valuep.unsafe_ptr()),
                Int(sizep.unsafe_ptr()),
            )
            if got == RD_KAFKA_RESP_ERR__NOENT:
                break
            self._lib.raise_if(got, "header_get_all")
            # `copy_bytes` and not a plain read: librdkafka leaves the value
            # pointer NULL for a header whose value is null, which is a
            # different header from one whose value is present and empty.
            out.append(Header(cstr(namep[0]), copy_bytes(valuep[0], sizep[0])))
            idx += 1
        return out^

    def commit(self, asynchronous: Bool = False) raises:
        """Commit the current offsets for this consumer group."""
        var rc = self._lib.commit(
            self._rk, Int32(1) if asynchronous else Int32(0)
        )
        self._lib.raise_if(rc, "commit")

    def close(mut self) raises:
        """Leave the group cleanly. Safe to call more than once."""
        if self._closed:
            return
        var rc = self._lib.consumer_close(self._rk)
        self._closed = True
        self._lib.raise_if(rc, "close")
