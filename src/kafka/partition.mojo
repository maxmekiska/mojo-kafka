"""Topic-partition addressing, and the offsets that go with it.

A `TopicPartition` is the currency of the consumer control plane: `assign`,
`seek`, `position`, `committed`, `pause`, `resume` and `offsets_for_times`
all speak in lists of them. It maps onto librdkafka's
`rd_kafka_topic_partition_t`, and like that struct it is used in **both
directions** -- a caller fills one in to ask a question, and the same field
comes back holding the answer.
"""

from ._ffi import (
    RD_KAFKA_OFFSET_BEGINNING,
    RD_KAFKA_OFFSET_END,
    RD_KAFKA_OFFSET_INVALID,
    RD_KAFKA_OFFSET_STORED,
    RD_KAFKA_RESP_ERR_NO_ERROR,
    KafkaErrorKind,
    kind_of,
)

# Offset sentinels, re-exported without librdkafka's prefix.
#
# They share the `Int64` with real offsets, so an offset field is never just
# a count. `OFFSET_INVALID` in particular is not an error: it is what
# `position()` reports for a partition nothing has been read from yet, and
# what `committed()` reports for a group that has committed nothing.
comptime OFFSET_BEGINNING: Int64 = RD_KAFKA_OFFSET_BEGINNING
comptime OFFSET_END: Int64 = RD_KAFKA_OFFSET_END
comptime OFFSET_STORED: Int64 = RD_KAFKA_OFFSET_STORED
comptime OFFSET_INVALID: Int64 = RD_KAFKA_OFFSET_INVALID


struct TopicPartition(Copyable, Movable, Writable):
    """One partition of one topic, at an offset.

        TopicPartition("events", 0)                    # no offset named
        TopicPartition("events", 0, OFFSET_BEGINNING)  # replay from the start
        TopicPartition("events", 0, 4096)              # replay from here

    `error_code` is **not** set by the constructor -- it is filled in when
    librdkafka hands one of these back. Several control-plane calls report
    per partition rather than through their return code, so a list that came
    back from `committed()` or `position()` may hold both good answers and
    failed ones; `has_error()` is how to tell them apart. The calls that
    return nothing (`seek`, `pause`, `resume`) raise on the first failure
    instead, because there would be nowhere to put it.

    Deliberately **not** `@fieldwise_init`: that would synthesise a
    four-argument constructor letting a caller invent an `error_code`, and
    the field only means anything when librdkafka wrote it.
    """

    var topic: String
    var partition: Int32
    var offset: Int64
    var error_code: Int32

    def __init__(
        out self,
        topic: String,
        partition: Int32,
        offset: Int64 = OFFSET_INVALID,
    ):
        """Name a partition, optionally at an offset.

        `OFFSET_INVALID` is librdkafka's own initial value for the field and
        means "no offset named" -- for `assign()` that is "start where the
        group left off", which is `OFFSET_STORED` behaviour.
        """
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.error_code = RD_KAFKA_RESP_ERR_NO_ERROR

    @staticmethod
    def _decoded(
        topic: String, partition: Int32, offset: Int64, error_code: Int32
    ) -> Self:
        """Rebuild one from a `rd_kafka_topic_partition_t`, error and all."""
        var tp = Self(topic, partition, offset)
        tp.error_code = error_code
        return tp^

    def has_error(self) -> Bool:
        """True if librdkafka reported a problem with *this* partition."""
        return self.error_code != RD_KAFKA_RESP_ERR_NO_ERROR

    def kind(self) -> KafkaErrorKind:
        """The branchable category of this partition's error.

        `error_code` keeps the exact librdkafka value; this is the one to
        branch on, the same way `DeliveryReport.kind()` is.
        """
        return kind_of(self.error_code)

    def write_to(self, mut writer: Some[Writer]):
        """`topic[partition]@offset`, with the error appended if there is one.
        """
        writer.write(
            self.topic, "[", self.partition, "]@", _offset_text(self.offset)
        )
        if self.has_error():
            writer.write(" err=", self.error_code)


def _offset_text(offset: Int64) -> String:
    """Render an offset, naming the sentinels rather than printing them.

    `-1001` in a log line is a puzzle; `invalid` is an answer.
    """
    if offset == OFFSET_BEGINNING:
        return String("beginning")
    if offset == OFFSET_END:
        return String("end")
    if offset == OFFSET_STORED:
        return String("stored")
    if offset == OFFSET_INVALID:
        return String("invalid")
    return String(offset)


@fieldwise_init
struct Watermarks(Copyable, Movable, Writable):
    """The first and last offsets a partition currently holds.

    `high` is the offset the **next** record will be given, not the offset of
    the last one written -- so a partition holding ten records that have
    never been deleted reports `low=0, high=10`, and a consumer that has read
    all ten reports `position=10`. Lag is therefore `high - position` with no
    off-by-one correction, and `low == high` means the partition is empty.

    `low` is not always 0: retention and compaction move it forward, so a
    replay from `OFFSET_BEGINNING` starts at `low`, not at zero.
    """

    var low: Int64
    var high: Int64

    def is_empty(self) -> Bool:
        """True when the partition holds no readable records."""
        return self.low == self.high

    def write_to(self, mut writer: Some[Writer]):
        writer.write("[", self.low, ", ", self.high, ")")
