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
