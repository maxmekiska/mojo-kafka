"""Hex codec for the interop wire contract.

Every peer -- Mojo or Python -- exchanges message bytes as lowercase hex, so
the harness compares **bytes** rather than decoded text. That matters here for
two reasons:

- Kafka keys and values are opaque byte arrays. Comparing decoded strings would
  hide exactly the class of bug this suite exists to find.
- Payloads in the fixture set carry tabs, newlines and NUL bytes, which would
  otherwise collide with the line-oriented transport between the peer processes
  and the test runner.

A single `-` means *null* (field absent), which Kafka distinguishes from an
empty byte array. See `../fixtures.py` for the contract.

Headers ride on the same codec: the field is `-` for a record with no headers,
or `<name_hex>:<value_hex>` pairs joined by `,`. Hex-encoding the *name* as
well is what stops a `,` or `:` inside a header name from breaking the framing.
"""

from kafka import Header

comptime NULL_FIELD = "-"


def hex_encode(data: Span[UInt8, _]) -> String:
    """Lowercase hex, no separators."""
    comptime DIGITS = "0123456789abcdef"
    var digits = DIGITS.as_bytes()
    var out = List[UInt8](capacity=len(data) * 2)
    for i in range(len(data)):
        var b = Int(data[i])
        out.append(digits[b >> 4])
        out.append(digits[b & 0xF])
    return String(unsafe_from_utf8=Span(out))


def _nibble(c: UInt8) raises -> Int:
    var v = Int(c)
    if v >= 48 and v <= 57:  # '0'-'9'
        return v - 48
    if v >= 97 and v <= 102:  # 'a'-'f'
        return v - 87
    if v >= 65 and v <= 70:  # 'A'-'F'
        return v - 55
    raise Error("not a hex digit: " + String(v))


def hex_decode(text: String) raises -> List[UInt8]:
    """Inverse of `hex_encode`. Raises on odd length or a non-hex digit."""
    var src = text.as_bytes()
    if len(src) % 2 != 0:
        raise Error("hex string has odd length: " + String(len(src)))
    var out = List[UInt8](capacity=len(src) // 2)
    for i in range(0, len(src), 2):
        out.append(UInt8(_nibble(src[i]) * 16 + _nibble(src[i + 1])))
    return out^


def headers_encode(headers: List[Header]) -> String:
    """Encode a header list for the wire; `-` when there are none."""
    if len(headers) == 0:
        return String(NULL_FIELD)
    var out = String("")
    for i in range(len(headers)):
        if i > 0:
            out += ","
        ref header = headers[i]
        out += hex_encode(header.name.as_bytes())
        out += ":"
        # A null header value is `-`; an empty one is the empty string, and
        # the two must not collapse into each other here of all places.
        if not header.value:
            out += NULL_FIELD
        else:
            out += hex_encode(Span(header.value.value()))
    return out^


def headers_decode(field: String) raises -> List[Header]:
    """Inverse of `headers_encode`."""
    var out = List[Header]()
    if field == NULL_FIELD:
        return out^
    for pair in field.split(","):
        var halves = String(pair).split(":")
        if len(halves) != 2:
            raise Error("malformed header pair: " + String(pair))
        var name = String(unsafe_from_utf8=Span(hex_decode(String(halves[0]))))
        var value = Optional[List[UInt8]](None)
        if String(halves[1]) != NULL_FIELD:
            value = hex_decode(String(halves[1]))
        out.append(Header(name, value^))
    return out^
