# Operate HTTP delivery receipts

Enable DLR only with RabbitMQ messaging and a reachable StateStore. Once enabled,
`JasminEx.Dlr.Supervisor` declares its topic topology before starting lookup and
HTTP callback consumers. The existing `/send` POST path registers correlation
before publishing MT work; a known `submit_sm_resp` or a parsed `deliver_sm`
receipt enters the DLR topic path. This does not add GET `/send`, `data_sm`, or an
SMPP callback thrower.

## Enable and verify

1. Configure the application's `:messaging` broker, `:state_store`, and `:dlr`
   settings before starting the release. Example application environment:

   ```elixir
   config :jasmin_ex,
     messaging: [enabled: true, host: "broker.example", username: "app", password: "secret"],
     state_store: [host: "valkey.example"],
     dlr: [enabled: true]
   ```

   Supply credentials through the deployment's secret mechanism, not in a
   committed configuration file. Configure the existing HTTP API, routing,
   and SMPP connectors separately; enabling DLR alone does not create them.
2. Inspect `JasminEx.Dlr.Readiness.status()` in the running node: `%{ready: true,
   error: nil}` means declaration succeeded and workers were started. Check
   broker consumer counts separately: a worker may still be retrying its own
   subscription. An error means the DLR path is unavailable; it does **not**
   imply a classic fallback.
3. On the RabbitMQ node, inspect without modifying queues:

   ```sh
   rabbitmqctl list_exchanges -p / name type durable
   rabbitmqctl list_queues -p / name type arguments messages consumers
   rabbitmqctl list_consumers -p /
   ```

   Expect one consumer on each DLR lookup and HTTP queue after readiness, zero
   before declaration succeeds, and no DLR consumers when disabled. Check the
   queue arguments against the configured retry settings. The vhost may differ
   from `/` if configured in `:messaging`.
4. Run `mix test --only integration test/jasmin_ex/dlr/e2e_test.exs` in a local
   environment with the repository's pinned RabbitMQ and Valkey images. The
   scenario exercises HTTP intake, production Redix, the broker, FakeSMSC,
   Mint, actual endpoint requests, broker restart, and DLR subtree restart.

## Configuration

All `:dlr` fields except `:http_client` are validated by `JasminEx.Dlr.Config`.
Values marked as attempts are *additional* attempts after the first delivery.

| Field | Default | Effect |
|---|---:|---|
| `enabled` | `false` | Keeps DLR child graph absent unless `true`; requires messaging enabled. |
| `queue_prefix` | `jasmin_ex.dlr` | Prefix for the durable DLR queues and DLX. |
| `lookup_additional_attempts` | `2` | Lookup worker budget; broker delivery limit is `3`. |
| `lookup_delay_ms` | `10000` | Lookup quorum delayed-retry minimum and maximum. |
| `http_additional_attempts` | `3` | HTTP worker budget; broker delivery limit is `4`. |
| `http_delay_ms` | `30000` | HTTP quorum delayed-retry minimum and maximum. |
| `http_timeout_ms` | `30000` | Mint's overall callback request timeout. |
| `dlr_expiry_s` | `86400` | Correlation and connector receipt lifetime. |

The validated production defaults use the existing `StateStore.Redix`,
`TopicPublisher`, `HttpClient.Mint`, and the system clock. Explicit `:store`,
`:connection_server`, `:publisher`, and `:http_client` overrides are injection
points for tests; the production HTTP client supplies **no** private-address
allow rules. A loopback allowance in the integration test is scoped to that
test and must not be copied into production configuration.

## Ownership and failure behavior

`messaging` is a durable topic exchange. `<prefix>.dlx` is a fanout dead-letter
exchange. `<prefix>.lookup.v1` binds `dlr.*`; `<prefix>.http.v1` binds
`dlr_thrower.http`; `<prefix>.dead.v1` binds the DLX. Lookup and HTTP are quorum
queues with a single active consumer, at-least-once dead-lettering, broker-owned
delayed retries, `reject-publish` overflow and the configured delivery limits.
The dead queue is durable quorum and is not a retry queue. These queues are
separate from MT work queues; the DLR workers never consume MT work.

If the broker is unavailable at startup, readiness retries with bounded
250–5000 ms backoff and does not start DLR consumers first. Connection/channel
loss removes the old consumer set; readiness redeclares topology before starting
new consumers. Each worker also retries an initially failed consume. Use
`Readiness.status/0`, broker queue consumer counts, and the warning
`DLR topology unavailable` to distinguish unavailable dependencies from an
idle queue. Worker settlements emit `[:jasmin_ex, :dlr, :settlement]` telemetry
with phase and reason class; sensitive callback details are excluded. The
messaging connection has separate recovery telemetry.

An incompatible `.v1` durable queue declaration (for example, changing retry
arguments on an existing queue) leaves the existing queue and its messages
untouched. Readiness reports `:incompatible_queue_arguments` when RabbitMQ
reports inequivalent arguments; unsupported delayed retry reports
`:delayed_retry_unsupported`. **Do not** delete/recreate the queue, change its
type to classic, or reuse a prefix with conflicting arguments as a shortcut.
Inspect the existing arguments and arrange a separately reviewed operator
migration if the deployment needs different values. WU7 does not implement one.

The StateStore owns expiring request/reverse maps and lookup plans. The local
test's Valkey state survived a graceful container stop/start; production host
failure durability depends on the deployed StateStore's persistence settings
and is not guaranteed by DLR. A store
outage causes lookup retries under the broker budget; it must not be mistaken
for missing correlation. A known submit response is checkpointed before event
publication, so ambiguous publication can be retried but is not exactly-once.
Plans record `planned`, `forwarded`, and `complete`; a failure between publish
and marking forwarded can cause a duplicate HTTP job. Endpoints must tolerate
repeated callbacks. A delivered callback requires the literal trimmed body
`ACK/Jasmin` and a status below 400 except 404, which is terminal. Other
failures retry within the broker-owned budget. There is one network attempt
per HTTP job, not an inner Mint retry loop.

HTTP callbacks support GET with compatible query fields and POST with
`application/x-www-form-urlencoded` fields (`id`, `level`, `message_status`,
`connector`, plus receipt fields for level 2). The destination policy rejects
private or otherwise disallowed resolved addresses by default, pins the
approved peer while preserving the original Host/TLS hostname, verifies TLS,
and does not follow redirects. Mint bounds connect time (default 5000 ms),
response headers (16384 bytes), and response body (65536 bytes). A redirect
response is classified by status and ACK body without following its Location.

## Multipart DLR — agreed design (not implemented)

This section records the agreed direction for [issue #91](https://github.com/Camilo-J/jasmin_ex/issues/91),
not a released feature or a complete implementation specification. Multipart
requests with enabled DLR still return HTTP 400 `multipart_dlr_not_supported`.
The configuration table above describes current behavior; the proposed deadline
below has no implemented configuration option yet.

### Callback contract

The agreed interface uses **POST with JSON only for both single-message and
multipart DLR callbacks**, with `Content-Type: application/json`. GET callbacks
and POST form encoding are not supported by the new contract; there is no
optional legacy transport or automatic fallback. This replaces the earlier
form-default and JSON-default-with-optional-legacy proposals. Existing receivers
using GET or form payloads must migrate to POST JSON. This is an intentional
breaking callback-interface change, not a change to the HTTP send endpoint.
The current implementation has not changed. HTTP QUERY is outside this design.

JSON callbacks retain the current receiver acknowledgement behavior: a status
below 400 and a body equal to `ACK/Jasmin` after trimming whitespace confirms
receipt. HTTP 404 remains terminal; other failures retain the existing retry
policy. Receivers should return HTTP 200 with body `ACK/Jasmin`. JSON does not
introduce a JSON acknowledgement body or tighten the existing acceptance rules.

Both message sizes use the same payload shape: the message result is always in
root `status`, and provider evidence is always in `segments`, even for one segment.
There is no `multipart` wrapper or duplicate root `message_status`. This replaces
the earlier nested `multipart.aggregate_status` proposal, without a public
`dlr-mode` selector or a second legacy multipart mode.

| Field | Meaning |
|---|---|
| `schema_version` | Integer payload schema version, initially `1`. |
| `id` | Gateway ID of the complete message. |
| `connector` | Gateway connector id that submitted the message. Same value on acceptance and delivery. Omitted only when no submit was proven. Not the bind that received the receipt, and not an SMSC message id. |
| `level` | Integer event stage: `1` for submission acceptance, `2` for delivery. |
| `status` | Gateway conclusion for this stage, not a raw SMSC status. |
| `status_origin` | Always `gateway`, including successful outcomes. |
| `reason` | Included when applicable; `DLR_TIMEOUT` identifies missing delivery evidence at the deadline, and `SUBMIT_UNCONFIRMED` identifies early delivery closure because a segment's submit was never confirmed. |
| `total_segments` | Number of required segments, including those without evidence. |
| `segments` | Every segment in index order, without deduplication by SMSC ID. |

Each segment has a one-based `index` and, when known, its `id_smsc`, original
provider `message_status`, and original provider `err`. A delivery receipt may
also include the SMSC counters `sub` and `dlvrd`, the opaque provider dates
`subdate` and `donedate`, and the provider `text`. Missing evidence is omitted
rather than converted into a fabricated provider failure. The placeholder `ND`
and an empty `text` are missing evidence.

`sub` and `dlvrd` are the SMSC's short-message counters from the receipt,
preserved as zero-padded three-digit text when the receipt contained one to
three digits. They count short messages the SMSC says it submitted and
delivered. They are not a segment index, not `total_segments`, and not proof
that a handset displayed the message. They do not affect aggregate `status`.
`subdate` and `donedate` are unparsed provider digit strings. Do not assume
their timezone, parse them into instants, or substitute them for `received_at`.
Configuring a provider timezone is not part of the agreed scope. `text` is the
receipt excerpt as sent by the SMSC. The SMPP example format describes the
first 20 characters of the short message, but providers vary and this contract
does not truncate, decode, or interpret it. It may contain message content.
Submission acceptance does not invent `sub`, `dlvrd`, `subdate`, `donedate`,
`text`, or `received_at`.

The current level-2 callback puts the raw SMSC id in `connector` for Jasmin
compatibility. The unified payload does not copy that quirk.

For delivery evidence, `received_at` is the gateway receipt-observation time in
UTC using an ISO 8601 timestamp with `Z`, not the HTTP callback send time. It is
preserved across callback retries.

Example of the agreed shape for one delivered segment:

```json
{
  "schema_version": 1,
  "id": "message-id",
  "level": 2,
  "status": "DELIVERED",
  "status_origin": "gateway",
  "connector": "smpp-t",
  "total_segments": 1,
  "segments": [
    {
      "index": 1,
      "id_smsc": "abc123",
      "message_status": "DELIVRD",
      "err": "000",
      "sub": "001",
      "dlvrd": "001",
      "subdate": "2601011200",
      "donedate": "2601011201",
      "text": "hello",
      "received_at": "2026-10-15T00:20:10.120Z"
    }
  ]
}
```

For multipart, the same fields apply with multiple entries in `segments`.

### Acceptance and delivery are separate events

Requested level `1` selects submission acceptance, level `2` selects delivery,
and level `3` selects both stages as separate callbacks. There is no callback
with `level: 3` combining the stages. HTTP retries can repeat each logical event;
two stages do not promise exactly two HTTP requests or exactly-once delivery.

There is one closed result per message and stage. The pair `(id, level)`
identifies that logical event and lets receivers deduplicate callback retries.
Retries preserve the same pair and payload; acceptance and delivery share `id`
but have different `level` values. No separate `event_id` field is included.

Submission acceptance uses `ACCEPTED` only when every required segment has a
successful `submit_sm_resp`. A definitive rejection or proven inability to send
a required segment yields `FAILED`. Missing submission evidence at the end of
the applicable wait yields `UNRESOLVED` when no definitive failure is known.
Segment `message_status` preserves provider codes such as `ESME_ROK`.

The agreed acceptance policy reuses the per-connector submit response timeout,
configurable through `response_timeout_ms` with a default of **5,000 ms**.
The current client starts this timer after `Transport.send` returns `:ok`;
it is a per-segment response wait, not a total multipart acceptance deadline
or a delivery-receipt timeout. Choose the value for the provider's response
latency rather than treating five seconds as a universal production limit.

Close aggregate acceptance after the permitted submission attempts for the
required segments have finished: all accepted means `ACCEPTED`; any definitive
failure means `FAILED`; otherwise, any uncertain acceptance means `UNRESOLVED`.
A response timeout does not prove rejection and does not authorize automatic
retransmission of an uncertain submit. The current client discards submit
responses received after its pending entry has timed out. Such a late response
can be a real acceptance, so the segment becomes `UNRESOLVED` rather than
`FAILED`, and its SMSC message id is lost, which prevents correlating a later
delivery receipt for that segment. Recovering late submit responses is a
recorded follow-up improvement, not part of the agreed scope; until then, tune
`response_timeout_ms` per connector for slow providers.

Delivery uses the outcomes below. An acceptance result of `ACCEPTED` followed by
a delivery result of `FAILED` is not contradictory: acceptance does not prove
delivery. A definitive submission failure must also allow the requested delivery
stage to close as `FAILED`, without waiting for an impossible receipt for the
unsent segment. Successful sibling segments retain their actual evidence.

| Aggregate outcome | Evidence |
|---|---|
| `DELIVERED` | Every required segment has a delivery receipt reporting `DELIVRD`. |
| `FAILED` | A required segment has a definitive failure; other segments retain their actual outcomes. |
| `UNRESOLVED` | The decision deadline expires with missing evidence and no known definitive failure (reason `DLR_TIMEOUT`), or, before the deadline, every segment is either decided or can never be correlated, with no known definitive failure (reason `SUBMIT_UNCONFIRMED`). |

Delivery closes as soon as its outcome can no longer change. A segment without
a known SMSC message id, because its dispatch stayed unresolved after a crash or
its submit response arrived after `response_timeout_ms` and was discarded, can
never be matched to a delivery receipt, so the message can no longer become
`DELIVERED`. When every segment is either decided or in that state and none has
failed, delivery closes immediately as `UNRESOLVED` with reason
`SUBMIT_UNCONFIRMED` instead of waiting for the deadline. A definitive failure
still closes it as `FAILED` immediately. While any segment can still receive a
correlatable receipt, the deadline applies as usual.

The aggregate is a gateway conclusion, not a status or error reported by the
SMSC. Acceptance for submission does not prove delivery, and segment delivery
does not guarantee that a handset displayed the complete message. One failed
segment does not require the SMSC to fail or cancel its siblings. Preserve the
original provider statuses and errors in per-segment details; do not manufacture
`UNKNOWN`, `EXPIRED`, or `UNDELIV` to represent a local timeout.

For conflicting delivery receipts for the same segment while the aggregate is
open, use the last receipt observed by the gateway, preserving its original
provider status and error. Gateway receipt order does not prove SMSC generation
order. Once a result closes, later receipts do not revise its outcome or frozen
segment evidence and do not generate a replacement callback. In particular,
`DELIVRD` after a closed `FAILED` result, or a failure after a closed `DELIVERED`
result, does not reopen it. Do not delay closure to wait for possible provider
corrections.

The root `id` is the gateway ID of the complete message. SMSC IDs appear only
in `segments[].id_smsc`, when known; the unified payload has no root `id_smsc`,
including for a single segment. Per-segment references make a representative
SMSC ID unnecessary. This replaces the earlier last-segment representative rule.

### Receipt correlation

Today the reverse correlation key is the pair of connector id and normalized
SMSC message id, and a receipt must arrive on the same connector that submitted
the message; otherwise it is never matched (`reverse_map_missing`) or is
discarded (`connector_mismatch`). Some providers submit through one bind and
deliver receipts through another bind of the same SMSC, so the agreed design
adds an explicit **correlation group**:

- A connector may declare a correlation group shared with other connectors of
  the same SMSC. The reverse key becomes the pair of correlation group and
  normalized SMSC message id, and a receipt received by any connector in the
  group can match a message submitted by another connector in that group.
- A connector without a declared group forms its own group, preserving today's
  per-connector isolation. Correlation by SMSC message id alone, across all
  connectors, was rejected because different SMSCs can issue the same id.
- The configuration name of the group is not decided.
- The root `connector` remains the connector that submitted the message, not the
  connector that received the receipt.

When an SMSC reuses a message id within a correlation group:

- If the message that holds the existing mapping is already closed, the new
  message takes over the mapping. The closed message only loses late-receipt
  recognition, which never changes a closed result.
- If that message is still open, the existing mapping is kept and the new
  segment becomes uncorrelatable, closing under the `SUBMIT_UNCONFIRMED` rule
  above. The collision is logged and counted so operators can detect it. A
  receipt is never attributed to a message it might not belong to.

### Deadline and operational retention

The agreed initial delivery decision deadline is **one hour, configurable**, from
the first segment's submit send to the SMSC, not broker queue confirmation or
the later submit response. Later segments do not restart the clock.

The send time is recorded durably in the existing per-segment `:dispatching`
settlement journal record, which is written before the submit is handed to the
SMPP client. This adds no extra store write per segment. The deadline starts at
the recorded time of the message's first segment. Because the record precedes
the actual socket write, the deadline may start a few milliseconds early, never
late. After a crash, recovery uses the recorded time and does not restart the
deadline; a segment left in `:dispatching` stays unresolved and is not
retransmitted, as today. Recording the time after `Transport.send` returns was
rejected: it adds a write per segment and loses the time if the process stops
between the send and that write. Segments without a proven submit close
delivery early as described under the delivery outcomes above.

Both the delivery decision deadline and the post-closure retention window have
a global default that each connector can override, following the existing
`dlr_expiry` pattern: the global value is injected into connectors that do not
set their own, and an explicit connector value wins. A message uses the values
of the connector that submitted it. It is an
operational cutoff for missing evidence, not a provider delivery guarantee, a
universal maximum, or an instruction to cancel the SMS. It does not change the
SMSC validity period.

The deadline is the governing clock for the unified contract. Correlation
retention is not an independent 24-hour clock: after a delivery result closes,
the state that recognizes late or duplicate receipts is kept only for a short,
user-configurable post-closure retention window, measured from closure. Because
closed results never reopen, longer retention would only distinguish late
receipts for closed messages from orphan receipts. Current `dlr_expiry_s`
(24-hour default) describes today's single-message correlation lifetime, not
this agreed design. The post-closure window defaults to **one hour** and is
configurable; its configuration name is not decided. HTTP callback retry state follows the
retention policy below and is not cut short by this window.

Once a delivery result is closed, a receipt arriving after the timeout does not
revise it or generate an updated result. HTTP retries retain the same event
payload. This deliberately trades late delivery updates for a stable callback
contract and minimal retention; `UNRESOLVED` does not mean proven delivery failure.

Recovery after a restart must preserve segment evidence, the original deadline,
and pending callback obligations. It must resume overdue decisions and callback
retries, not restart the deadline or retransmit SMS. Redis key expiry alone does
not generate a timeout callback, and there is no scheduler today.

#### Deadline wake-up

The deadline is fired by a durable RabbitMQ wake-up message, reusing the
existing wait-queue pattern (message TTL plus dead-lettering into a work queue)
already used for retries:

- When a message's first segment is dispatched, one wake-up message carrying the
  gateway id and the exact deadline is published. There is one wake-up per
  message, not per segment, and only for the delivery stage; acceptance closes
  when the submit attempts finish.
- The wake-up always arrives. Its consumer reads the state: if the result is
  already closed (for example `DELIVERED`, `FAILED`, or `SUBMIT_UNCONFIRMED`),
  it does nothing; otherwise it closes the result as `UNRESOLVED` with
  `DLR_TIMEOUT` and follows the closed-state record steps. Wake-ups are never
  cancelled.
- The triggering message, a wake-up or a receipt, is acknowledged only after the
  closed-state record is shrunk, so a crash leads to redelivery and the same
  frozen payload is republished.
- Wait queues are a fixed set of tiers with a queue-level TTL, initially 1 hour,
  10 minutes, 1 minute, and 10 seconds. A wake-up is published to the largest
  tier that does not exceed the remaining time; when it expires, the consumer
  closes the result if the deadline has passed or republishes the wake-up to the
  next fitting tier. Every message in a tier has the same TTL, so expiry order
  is preserved, and any configured deadline works without creating queues.
  Closure may happen up to about the smallest tier (10 seconds) late.
- One queue per configured deadline value was rejected: queue expiry
  (`x-expires`) deletes a queue without consumers even while it holds pending
  messages, and RabbitMQ documents queue TTL as suitable for transient queues,
  so obsolete per-deadline queues could not be cleaned up safely.
- Empty queues cost almost nothing; each waiting wake-up is small, roughly 1 KB
  including RabbitMQ metadata, and classic v2 queues keep most waiting messages
  on disk. This is an estimate from RabbitMQ documentation, not a measurement.
- Implementation notes: declare the work queue before any wait queue can expire
  into it, because a dead-lettered message without a target is dropped; confirm
  the dead-lettering guarantees of the chosen queue type.

Open aggregation records in Redis carry a safety TTL of the deadline plus the
post-closure window plus a margin, so state cannot be left behind if a wake-up
is lost.

The intended storage policy is minimal operational retention, not a full archive
of late receipts:

1. Keep the per-segment evidence needed while aggregation is active.
2. Preserve pending callback intent and the state needed for retries and recovery
   until those obligations are resolved; deciding an outcome is not enough to
   safely delete that state.
3. Afterwards, retain only bounded closed-state and correlation information needed
   to recognize late or duplicate receipts. Do not extend its TTL on every late
   receipt or retain a complete late-receipt history by default.

#### Closed-state record

HTTP callback jobs are already self-contained messages on the `dlr_thrower.http`
RabbitMQ queue, carrying the URL, fields, and their own deadline, and that queue
owns callback retries. Pending callback obligations therefore do not need to
stay in Redis once the job is published. For each event `(id, level)`:

1. On closure, write a closed record with `status`, `reason`, `closed_at`, the
   frozen callback payload, and a pending-publication flag.
2. Publish the callback job to `dlr_thrower.http`.
3. After the broker confirms the publication, shrink the record to a compact
   marker with `status`, `reason`, and `closed_at`, and delete the frozen
   payload and the per-segment evidence used while the result was open.
4. The marker and the reverse keys (correlation group and SMSC message id)
   expire at `closed_at` plus the post-closure retention window.

If the process stops between steps 1 and 3, recovery finds the pending flag and
republishes the same frozen payload; receivers deduplicate by `(id, level)`.
During the window, a late receipt resolves through its reverse key to a closed
marker and is only logged and counted, without a callback. The same marker
tells the reused-id rule that the previous message is closed. After the window,
nothing remains and a late receipt is recorded as an orphan.

Production currently uses Redis through `StateStore.Redix`. Capacity planning
must account for active and retained messages, segments per message, correlation
keys, serialized values, operation volume, and deployment overhead. A compact
closed marker does not eliminate the cost of correlation keys. No memory or cost
estimate has been measured for the proposed aggregate records yet.

### Decisions still open

- Configuration names for the delivery decision deadline, the post-closure
  retention window, and the correlation group, and the margin of the safety TTL
  for open records. These are implementation-level details.

These decisions must be resolved before implementation. This design does not
authorize automatic retransmission, cancellation, refunds, or exactly-once claims.
Documentation organization can be revisited once the contract is complete.

## Troubleshoot and roll back

| Symptom | Check | Safe action |
|---|---|---|
| Readiness error and zero consumers | Broker connection, quorum delayed-retry support, queue arguments | Restore broker/support; do not use classic fallback or delete `.v1` queues. |
| Missing correlation | StateStore connection, request expiry, POST `/send` result | Restore StateStore; check expiry and the MT publisher separately. |
| No receipt event | FakeSMSC/SMSC bind, `submit_sm_resp` checkpoint and `deliver_sm` ACK | Verify connector DLR publisher and the `dlr.*` binding; do not route receipts to MT. |
| HTTP callback retries | Destination approval, DNS, TLS certificate, response status/body, queue redelivery count | Fix the endpoint or policy configuration; ACK with `ACK/Jasmin`. |

To disable or roll back, set `dlr: [enabled: false]` and restart the application
to stop DLR consumers. Keep RabbitMQ DLR queues, dead letters, MT work, and
StateStore mappings/plans until jobs are drained, quarantined, expired, or
resolved under an explicit operator policy. Revert only WU7 production wiring
and its tests/docs if needed; WU1–WU6 domain contracts and stored records must
remain intact. No destructive queue or data migration is part of this work.

Out of scope: `data_sm`, SMPP callback thrower, GET `/send` DLR, per-request
expiry overrides, changes to MT routing/billing/queues, and exactly-once
callback guarantees.
