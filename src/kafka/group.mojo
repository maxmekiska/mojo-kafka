"""Consumer group identity, shared by both sides.

One type, and it exists because exactly-once needs the two clients to talk
about the same group: the offsets a transaction commits belong to the
*consumer's* group, but they are sent by the **producer**, inside the
transaction. `rd_kafka_consumer_group_metadata` is how librdkafka carries
that identity across, and this is its owner.

    var metadata = consumer.consumer_group_metadata()
    _ = producer.send_offsets_to_transaction(
        consumer.position(assignment), metadata
    )

Lives in its own module for the same reason `header.mojo` does -- it belongs
to neither client and is used by both.
"""

from ._ffi import Lib


struct ConsumerGroupMetadata:
    """An owned `rd_kafka_consumer_group_metadata_t*`.

    Obtained from `Consumer.consumer_group_metadata()` and passed to
    `Producer.send_offsets_to_transaction()`. There is no public constructor:
    the identity is librdkafka's to state, and one built by hand would name a
    group this process is not actually a member of.

    **Not copyable, deliberately.** The handle is caller-owned and freed once
    in `__deinit__`; a copy would double-free. Move it (`metadata^`) if it
    has to change hands.

    It is a **snapshot**, not a live view. A rebalance changes the consumer's
    generation, and librdkafka rejects offsets sent under a stale one, so
    take it inside the same transaction that sends the offsets rather than
    once at startup.
    """

    var _lib: Lib
    var _handle: Int

    def __init__(out self, handle: Int) raises:
        """Internal. `Consumer.consumer_group_metadata()` is the way in.

        It opens its own `Lib` rather than borrowing the consumer's, because
        `Lib` is `Movable` but not `Copyable` -- every client here does the
        same, and `dlopen` refcounts, so librdkafka is still mapped once. The
        cost is the symbol binding, microseconds against a transaction's
        network round trip.
        """
        if handle == 0:
            raise Error(
                "rd_kafka_consumer_group_metadata returned NULL -- the"
                " consumer has no group.id"
            )
        self._lib = Lib()
        self._handle = handle

    def __deinit__(deinit self):
        # Destructors cannot raise. `_lib` is named here, which is what keeps
        # it alive across the free -- see "Lifetimes" in CLAUDE.md.
        try:
            if self._handle != 0:
                self._lib.consumer_group_metadata_destroy(self._handle)
        except:
            pass

    def _address(self) -> Int:
        """The raw pointer, for the one call that consumes it."""
        return self._handle
