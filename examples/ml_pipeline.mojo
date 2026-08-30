"""Hero example: a streaming ML feature pipeline.

Consumes events off Kafka, pulls a numeric feature out of each one, runs
a toy "inference" step, and prints the prediction. Replace the body of
`run_inference` with a real MAX/Mojo model -- the streaming layer above
it does not change.

This is the whole reason `mojo-kafka` exists: keep the data path in Mojo
end to end so you do not pay a Python hop per message.
"""

from std.math import exp

from kafka import Consumer, ConsumerConfig


def parse_feature(value: String) -> Float64:
    """Pull the first numeric token out of `value`."""
    var acc = String("")
    var seen_digit = False
    for ch in value.codepoints():
        var c = String(ch)
        if ch.is_ascii_digit() or c == "." or (not seen_digit and c == "-"):
            acc += c
            seen_digit = True
        elif seen_digit:
            break
    if acc.byte_length() == 0:
        return 0.0
    try:
        return Float64(acc)
    except:
        return 0.0


def run_inference(feature: Float64) -> Float64:
    """Stand-in for a real model. Logistic over a single input."""
    return 1.0 / (1.0 + exp(-feature))


def main() raises:
    var c = Consumer(
        ConsumerConfig(
            bootstrap_servers="localhost:9092",
            group_id="mojo-ml-pipeline",
            auto_offset_reset="earliest",
        )
    )
    c.subscribe(["features"])

    print("Listening on 'features' -- Ctrl-C to stop.")
    while True:
        var maybe = c.poll(timeout_ms=1000)
        if not maybe:
            continue
        ref m = maybe.value()
        var x = parse_feature(m.value_text())
        var y = run_inference(x)
        print("offset=", m.offset, " x=", x, " y_hat=", y)
