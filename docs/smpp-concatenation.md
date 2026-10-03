# SMPP concatenation codec

`SubmitSM.optional_parameters` now carries binary TLV bytes after `short_message`,
just like `DeliverSM`. Empty optional bytes preserve ordinary SubmitSM wire output.
This is codec support only: it does not split messages or activate HTTP submission,
planner integration, queueing, billing, retries, or delivery-receipt aggregation.

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
