# Pure SMS segment planning

`JasminEx.MtSubmitPipeline.Segmentation.plan/3` plans already-encoded SMS bytes without transport, randomness, billing, or queue side effects. **HTTP multipart submission is not active.** Existing HTTP/PDU limits and wire behavior are unchanged.

`JasminEx.Billing.Bill.new/1` optionally accepts `segment_count` as an integer `1..255`. Omit it to keep the existing single-segment bill. `rate_minor` is still the per-segment unit price; the constructed bill stores `unit * N` as total `rate_minor` and `N` as `quota_debit`. Rounding splits the unit rate first, then multiplies precharge and remainder by `N`. Explicit `nil` or any other invalid count returns `{:error, :invalid_segment_count}`. A total past signed int64 returns `{:error, :amount_overflow}`. Count `255` is the representational protocol bound; the HTTP planner's configurable user max (default `5`) is unchanged. This constructor does not activate HTTP multipart billing.

`JasminEx.Billing.SegmentLedger` is a pure, opt-in in-memory ledger for per-segment settlement deltas. It is not wired to reservations, account credit, retries, or HTTP. `open/1` binds a valid `Bill` and its recomputed fingerprint; `record/5` returns refund and quota deltas only. Replay from a stale copy is not durable or idempotent across process restart: the consumer must atomically persist the new ledger and apply each delta. HTTP multipart submission remains inactive.

## API

```elixir
alias JasminEx.MtSubmitPipeline.Segmentation
{:ok, plan} = Segmentation.plan(:binary.copy("a", 161), 0, reference: 42)
# plan.count == 2; plan.segments are ordered, one-based segment maps
```

Options are `concat: :udh | :sar` (default `:udh`), `max_segments: 1..255` (default `5`), and `reference:` (required for multipart). The caller owns reference allocation and collision avoidance. UDH references are `0..255`; SAR references are `0..65535`. Single messages preserve their exact payload, `esm_class: 0`, and omit all concatenation fields, including when SAR is selected. Empty valid payloads produce one unchanged segment.

Every result segment contains `index`, `count`, `short_message`, and `esm_class`. Multipart UDH prepends `<<5, 0, 3, reference, count, index>>` to `short_message` and sets `esm_class: 0x40`. Multipart SAR leaves payload bytes unprefixed and `esm_class: 0`, adding `sar_msg_ref_num`, `sar_total_segments`, and `sar_segment_seqnum`. These are planning fields, not evidence of TLV serialization support.

## Coding and capacity

| Numeric coding | Unit | Single | UDH multipart | SAR multipart |
| --- | --- | ---: | ---: | ---: |
| `0` GSM 03.38 | Unpacked septets (one encoded octet each) | 160 | 153 | 152 |
| `2` raw binary / `3` Latin-1 | Octets | 140 | 134 | 133 |
| `8` UTF-16BE | Octets, complete codepoints | 140 | 134 | 132 |

These are SMS capacities, **not the 254-octet SMPP `short_message` ceiling**. GSM requires a provider accepting the project's unpacked GSM convention and correctly packing septets, including UDH alignment. Extension characters consume two septets; ESC pairs never split. UTF-16BE supplementary characters consume four octets; valid surrogate pairs never split. A segment can therefore leave unused capacity.

UDH uses the six-octet eight-bit-reference concatenation header. SAR conservatively reserves seven handset UDH octets for a sixteen-bit reference: `140 - 7 = 133` binary octets, `floor((1120 - 56) / 7) = 152` GSM septets, or 132 complete UTF-16 octets. SAR TLVs do not enlarge handset capacity. Provider integration must confirm SMSC concatenation handling and any additional header overhead before activation; this planner assumes no other user-data headers. The handset header layouts are defined in 3GPP TS 23.040, concatenated short-message information elements (`0x00` and `0x08`); see the [header layout summary](https://en.wikipedia.org/wiki/Concatenated_SMS#Sending_a_concatenated_SMS_using_a_User_Data_Header).

The encoding contract comes from `lib/jasmin_ex/smpp/pdu/coding.ex`: GSM is unpacked, Latin-1 is single-octet, binary is unchanged, and coding `8` accepts strict UTF-16BE including surrogate pairs. ASCII coding `1` and the codec's other raw-pass-through codings return `{:error, :unsupported_coding}` because their handset alphabet/packing semantics are not established here. Numeric codings only are accepted; no text is re-encoded.

## Rejection and boundaries

Malformed GSM or UTF-16BE returns `{:error, :invalid_payload}`, even for single messages. Invalid, duplicate, or unknown options return `{:error, :invalid_options}`; invalid references return `{:error, :invalid_reference}`. Overflow returns `{:error, {:too_many_segments, actual_count, max_segments}}`; bytes are never truncated and no partial plan is returned. Limits count actual safe segments, not an estimate from character length.

This API does not activate HTTP segmentation, serialize SAR TLVs, update envelopes, bill segments, dispatch queues, implement retries, or aggregate multipart delivery receipts. Those integration boundaries require separate work.
