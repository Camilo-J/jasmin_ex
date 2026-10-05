# SMPP concatenation codec

`SubmitSM.optional_parameters` carries binary TLV bytes after `short_message`,
just like `DeliverSM`. The queue envelope transports those bytes as v2
`optional_parameters_base64`, and one-octet `esm_class` including UDHI `0x40`,
through retries and quarantine. Empty optional bytes and default `esm_class` `0`
keep ordinary SubmitSM and envelope v2 wire output unchanged. HTTP remains
single-message: this does not split messages or activate HTTP multipart
submission, planner integration, billing, or delivery-receipt aggregation.

## Encode SAR parameters

```elixir
{:ok, optional} = JasminEx.Smpp.PDU.Tlv.encode(
  sar_msg_ref_num: 42, sar_total_segments: 2, sar_segment_seqnum: 1
)
body = %JasminEx.Smpp.PDU.Body.SubmitSM{
  short_message: "segment bytes", optional_parameters: optional
}
```

The reference is an unsigned 16-bit integer (0–65535); total and sequence are
unsigned 8-bit integers (1–255). All three parameters must appear together,
and sequence must not exceed total. SAR parameters cannot accompany UDHI (`0x40`).
UDH-only messages still round-trip with their existing `esm_class` and payload bytes.

## Queue envelope

In-memory `submit_sm.optional_parameters` is a binary. Encode writes canonical
padded `optional_parameters_base64` only when those bytes are non-empty. Absent
v1/v2 fields stay valid. Raw v2 `optional_parameters` and noncanonical Base64
are rejected. Envelope transport preserves unknown TLV bytes and does not
repeat PDU SAR validation.

In-memory `submit_sm.esm_class` is an integer 1–255 when present. Absent, v2
JSON-null, and `0` stay off the map and ordinary v2 wire; SubmitSM defaults
`esm_class` to `0`. Atom and string keys are copied. Envelope transport does
not validate UDHI or SAR coexistence. `struct(SubmitSM, envelope.submit_sm)`
remains the encode boundary. Worker and queue retries copy `submit_sm` as a whole.

## Decode and validation boundaries

- `Tlv.decode/1` returns ordered tuples: SAR names with integers, unknown numeric
  tags with binary values. It rejects truncation, incorrect SAR widths, and duplicate
  known singleton tags. It does not require a complete or semantically valid SAR trio.
- `Tlv.validate_sar/2` checks semantics against an `esm_class` (default `0`).
  `Tlv.encode/1` checks semantics too and returns a binary; numeric-tag/binary tuples
  allow unknown parameters, preserving their order and repeated unknown tags.
- SubmitSM body encode/decode validates optional bytes and wraps TLV errors as
  `{:error, {:encode | :decode, reason}}`. Body bytes remain lossless, including unknowns.
  DeliverSM keeps its existing raw-byte contract without new semantic rejection.

Malformed headers/values return `:truncated`; wrong SAR widths return
`:invalid_sar_length`. Other errors distinguish duplicate tags, invalid integer
ranges, incomplete/inconsistent SAR, oversized TLV values, and SAR with UDHI.
Existing receipt-tag classification is unchanged; decoded receipt tuples are not
a lossless representation of arbitrary original receipt bytes.

Protocol evidence: [SMPP 3.4 Issue 1.2](https://smpp.org/SMPP_v3_4_Issue1_2.pdf),
§5.2.12 (GSM SAR/UDHI exclusion), §5.3.2.22–24 (SAR field widths and ranges).
