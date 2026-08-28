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
    RD_KAFKA_RESP_ERR__PARTITION_EOF,
    _load_i32,
    _load_i64,
    _load_word,
    bytes_to_string,
)
from .config import ConsumerConfig


@fieldwise_init
struct Message(Copyable, ImplicitlyCopyable, Movable):
    var topic: String
    var partition: Int32
    var offset: Int64
    var key: String
    var value: String


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
        var list = self._lib.topic_partition_list_new(Int32(len(topics)))
        for topic in topics:
            _ = self._lib.topic_partition_list_add(list, topic, -1)
        var rc = self._lib.subscribe(self._rk, list)
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
            raise Error("poll: " + e.describe())

        # Every exit from here on has to destroy `raw`, including the ones
        # taken by a raising decode.
        var msg: Message
        try:
            msg = Message(
                self._lib.topic_name(_load_word(raw + MSG_RKT)),
                _load_i32(raw + MSG_PARTITION),
                _load_i64(raw + MSG_OFFSET),
                bytes_to_string(
                    _load_word(raw + MSG_KEY), _load_word(raw + MSG_KEY_LEN)
                ),
                bytes_to_string(
                    _load_word(raw + MSG_PAYLOAD), _load_word(raw + MSG_LEN)
                ),
            )
        except e:
            self._lib.message_destroy(raw)
            raise e
        self._lib.message_destroy(raw)
        return msg

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
