"""Record headers, and the UTF-8 decoder the record types share.

Headers are the third field of a Kafka record, alongside the key and the
value, and they follow the same null-versus-empty rules as those two -- see
"Null is not empty" in `Message`.
"""


@fieldwise_init
struct Header(Copyable, Movable):
    """One record header: a name, and a value that may be absent.

    Headers are a **list of pairs, not a map**. Kafka permits the same name
    to appear more than once and preserves the order they were written in, so
    a `Dict[String, String]` would silently drop all but one of a repeated
    name -- which is how conventions like tracing baggage and multi-value
    routing hints are carried. A list keeps both the duplicates and the order.

    `value` is `Optional` for the same reason `Message.value` is: librdkafka
    reads a header value's presence from the **pointer**, not its length, so
    a null value and a present-but-empty one are different headers on the
    wire. `value_text()` collapses null onto its `default`, so read `value`
    directly whenever that difference is the thing under test.

        Header("content-type", "application/json")   # text, present
        Header("trace-id", None)                     # present name, null value
        Header("frame", Optional(raw_bytes^))        # arbitrary bytes
    """

    var name: String
    var value: Optional[List[UInt8]]

    def __init__(out self, name: String, value: String):
        """A header whose value is text -- the common case.

        `byte_length()` rather than `len()`: Kafka counts bytes and `len()`
        on a `String` counts codepoints.
        """
        self.name = name
        self.value = List[UInt8](value.as_bytes())

    def value_text(self, default: String = "") -> String:
        """The value decoded as UTF-8, or `default` if the value was null."""
        return _text(self.value, default)


def _text(field: Optional[List[UInt8]], default: String) -> String:
    """Decode an optional Kafka field as UTF-8, mapping null to `default`.

    Shared by `Header.value_text` and by `Message.key_text` /
    `value_text` -- a header value, a key and a value are the same kind of
    field, and all three collapse null onto the caller's default here.

    The bytes are handed over unvalidated, exactly as they arrived. Kafka
    does not promise UTF-8, so a caller that cares should read the bytes.
    """
    if not field:
        return default
    return String(unsafe_from_utf8=Span(field.value()))
