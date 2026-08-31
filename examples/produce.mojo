"""Produce: a sensor emitting batches of readings.

    pixi run example-produce

Each record is a packed array of 64 `Float32` samples -- the shape telemetry
actually arrives in, and what `pipeline.mojo` reads back without copying.
"""

from kafka import KIND_QUEUE_FULL, Producer, ProducerConfig

comptime SAMPLES = 64
comptime TOPIC = "sensor.readings"


def samples(sensor: Int, batch: Int) -> List[UInt8]:
    """`SAMPLES` floats, packed little-endian, as the record's value."""
    var buf = List[UInt8](length=SAMPLES * 4, fill=0)
    var f = buf.unsafe_ptr().unsafe_bitcast[Float32]()
    for i in range(SAMPLES):
        f[unsafe_offset=i] = 20.0 + Float32(sensor) + 0.1 * Float32(batch + i)
    return buf^


def main() raises:
    var p = Producer(
        ProducerConfig(bootstrap_servers="localhost:9092", linger_ms=5)
    )

    for batch in range(50):
        for sensor in range(4):
            # `produce()` only enqueues; the broker's verdict arrives later,
            # which is why `flush()` below is what actually confirms delivery.
            var name = String("sensor-" + String(sensor))
            try:
                _ = p.produce_bytes(
                    topic=TOPIC,
                    value=samples(sensor, batch),
                    key=List[UInt8](name.as_bytes()),
                )
            except e:
                # The one rejection worth handling rather than raising: the
                # local queue is full, so drain it and the record fits.
                if p.last_error_kind() != KIND_QUEUE_FULL:
                    raise e
                _ = p.poll(100)

    # Raises if any record was rejected -- an undelivered message must never
    # look like a delivered one.
    p.flush(10000)
    print("produced 200 batches of", SAMPLES, "samples to", TOPIC)
