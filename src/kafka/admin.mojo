"""Admin client -- create and list topics."""

from ._ffi import (
    Lib,
    META_TOPICS,
    META_TOPIC_CNT,
    META_TOPIC_STRIDE,
    PTR_STRIDE,
    RD_KAFKA_PRODUCER,
    RD_KAFKA_RESP_ERR_NO_ERROR,
    _load_i32,
    _load_word,
    cstr,
)


struct AdminClient:
    var _lib: Lib
    var _rk: Int

    def __init__(out self, bootstrap_servers: String) raises:
        self._lib = Lib()
        var conf = self._lib.conf_new()
        try:
            self._lib.conf_set(conf, "bootstrap.servers", bootstrap_servers)
        except e:
            self._lib.conf_destroy(conf)
            raise e
        self._rk = self._lib.new_client(RD_KAFKA_PRODUCER, conf)

    def __deinit__(deinit self):
        # Destructors cannot raise.
        if self._rk != 0:
            try:
                self._lib.destroy(self._rk)
            except:
                pass

    def create_topic(
        self,
        name: String,
        num_partitions: Int32 = 1,
        replication_factor: Int32 = 1,
        timeout_ms: Int32 = 10000,
    ) raises:
        """Create a topic and wait for the broker's verdict.

        `rd_kafka_CreateTopics` is asynchronous and delivers its result to a
        queue, which is not optional -- passing NULL for it faults inside
        librdkafka. We make a queue, wait on it, and surface whatever the
        broker said rather than assuming success.

        The verdict has **two** levels, and checking only the outer one is a
        silent lie: `rd_kafka_event_error` reports whether the *request*
        failed, while a topic the broker refused -- already exists, invalid
        replication factor -- comes back with `RD_KAFKA_RESP_ERR_NO_ERROR` at
        the request level and the real error attached per topic inside the
        result.
        """
        # `new_topic` is owned from here on, and every call below can raise:
        # `queue_new`, `create_topics` and `queue_poll` as much as the decode.
        # One guard covers them all rather than a destroy per exit.
        var new_topic = self._lib.new_topic_new(
            name, num_partitions, replication_factor
        )
        var queue = 0
        var event = 0
        var failure: String
        try:
            var topics = Array[Int, 1](fill=new_topic)
            queue = self._lib.queue_new(self._rk)
            # Checked rather than assumed: `rd_kafka_CreateTopics` faults
            # inside librdkafka when handed a NULL queue, so a failed
            # allocation here has to become an exception before the call,
            # not a crash inside it.
            if queue == 0:
                raise Error("rd_kafka_queue_new returned NULL")
            self._lib.create_topics(
                self._rk, Int(topics.unsafe_ptr()), 1, queue
            )
            _ = topics^

            event = self._lib.queue_poll(queue, timeout_ms)
            failure = self._verdict(event, name) if event != 0 else String(
                "CreateTopics(" + name + "): timed out"
            )
        except e:
            if event != 0:
                self._lib.event_destroy(event)
            if queue != 0:
                self._lib.queue_destroy(queue)
            self._lib.new_topic_destroy(new_topic)
            raise e

        if event != 0:
            self._lib.event_destroy(event)
        self._lib.queue_destroy(queue)
        self._lib.new_topic_destroy(new_topic)
        if failure != "":
            raise Error(failure)

    def _verdict(self, event: Int, name: String) raises -> String:
        """Empty if the broker created the topic, otherwise why it did not."""
        var code = self._lib.event_error(event)
        if code != RD_KAFKA_RESP_ERR_NO_ERROR:
            return (
                "CreateTopics("
                + name
                + "): "
                + self._lib.event_error_string(event)
            )

        var result = self._lib.event_create_topics_result(event)
        if result == 0:
            return "CreateTopics(" + name + "): reply was not a topic result"

        var cnt_out = Array[Int, 1](fill=0)
        var arr = self._lib.create_topics_result_topics(
            result, Int(cnt_out.unsafe_ptr())
        )
        var cnt = cnt_out[0]
        if arr == 0 or cnt == 0:
            return "CreateTopics(" + name + "): broker returned no verdict"

        for i in range(cnt):
            var tr = _load_word(arr + i * PTR_STRIDE)
            if tr == 0:
                continue
            if self._lib.topic_result_error(tr) != RD_KAFKA_RESP_ERR_NO_ERROR:
                return (
                    "CreateTopics("
                    + self._lib.topic_result_name(tr)
                    + "): "
                    + self._lib.topic_result_error_string(tr)
                )
        return String("")

    def list_topics(self, timeout_ms: Int32 = 5000) raises -> List[String]:
        """Return the topic names this client can see."""
        var meta_out = Array[Int, 1](fill=0)
        var rc = self._lib.metadata(
            self._rk, 1, Int(meta_out.unsafe_ptr()), timeout_ms
        )
        self._lib.raise_if(rc, "metadata")

        var meta = meta_out[0]
        var topic_cnt = _load_i32(meta + META_TOPIC_CNT)
        var topics = _load_word(meta + META_TOPICS)

        # The walk decodes C strings and can raise, so every exit from here
        # has to release the metadata -- including the raising ones.
        var out = List[String]()
        try:
            for i in range(Int(topic_cnt)):
                # Each rd_kafka_metadata_topic_t is 32 bytes and starts with
                # `char *topic`.
                var name_ptr = _load_word(topics + i * META_TOPIC_STRIDE)
                if name_ptr != 0:
                    out.append(cstr(name_ptr))
        except e:
            self._lib.metadata_destroy(meta)
            raise e
        self._lib.metadata_destroy(meta)
        return out^
