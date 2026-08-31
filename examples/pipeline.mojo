"""Consume, process, produce -- the loop this package exists for.

    pixi run example-pipeline

Reads the raw sensor batches, reduces each one to a mean and a max, and
writes a summary back to Kafka. Two things make this the interesting example:

- **The samples are never copied.** `consume_borrowed()` lends `Span`s
  pointing straight into librdkafka's own receive buffer, so a 256-byte
  record costs no allocation and no memcpy on the way in. The batch owns the
  messages and the compiler will not let a span outlive it.
- **The reduction is SIMD.** Eight samples per instruction, over those same
  borrowed bytes. This is the half a Python client cannot do at all: there,
  every record crosses back into the interpreter before you touch it.
"""

from kafka import Consumer, ConsumerConfig, Producer, ProducerConfig

comptime LANES = 8
comptime IN = "sensor.readings"
comptime OUT = "sensor.summary"


def reduce_batch(
    bytes: Span[UInt8, _],
) -> Tuple[Float32, Float32]:
    """Mean and max of the packed samples, read in place, eight at a time.

    Takes the span with its origin unbound (`_`), so it accepts a view into
    librdkafka's buffer without the caller having to name that lifetime.
    """
    var n = len(bytes) // 4
    var f = bytes.unsafe_ptr().unsafe_bitcast[Float32]()
    var total = SIMD[DType.float32, LANES](0)
    var peak = SIMD[DType.float32, LANES](Float32.MIN)
    for i in range(0, n - LANES + 1, LANES):
        var lane = f.unsafe_load[width=LANES](i)
        total += lane
        peak = max(peak, lane)
    return (total.reduce_add() / Float32(n), peak.reduce_max())


def main() raises:
    var consumer = Consumer(
        ConsumerConfig(
            bootstrap_servers="localhost:9092",
            group_id="sensor-pipeline",
            auto_offset_reset="earliest",
        )
    )
    consumer.subscribe([IN])
    var producer = Producer(
        ProducerConfig(bootstrap_servers="localhost:9092", linger_ms=5)
    )

    var done = 0
    while done < 200:
        var batch = consumer.consume_borrowed(256, timeout_ms=1000)
        for i in range(len(batch)):
            var record = batch[i]
            var payload = record.value()
            if not payload:
                continue
            var stats = reduce_batch(payload.value())
            # Key the summary by the *sensor*, not the topic: it is what
            # partitions the output, so every reading from one sensor stays
            # ordered behind the last. Copied into a `String` here because it
            # has to outlive the batch these spans point into.
            var sensor = record.key()
            _ = producer.produce(
                topic=OUT,
                key=String(
                    unsafe_from_utf8=sensor.value()
                ) if sensor else "unknown",
                value='{"mean":'
                + String(stats[0])
                + ',"max":'
                + String(stats[1])
                + "}",
            )
            done += 1
        # The batch -- and every span taken from it -- dies here, which is
        # exactly when librdkafka's messages are released.
        _ = batch^

    producer.flush(10000)
    consumer.close()
    print("summarised", done, "batches into", OUT)
