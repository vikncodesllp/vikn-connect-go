# vikn-connect-go

Shared Go code for **Vikn Connect**, the cross-app integration bus: NATS
JetStream carrying CloudEvents 1.0 on `vikn.>`. Design and build sequence:
`auth_go/docs/vikn-connect.md`.

Every Vikn app that publishes or consumes events imports this module instead
of carrying its own copy of the envelope, outbox, or consumer loop. That is
the only mechanism that keeps the dedupe, the ack discipline and the actor
check identical across repos.

| Package | What it owns |
|---|---|
| `connect` (root) | `Envelope` (CloudEvents + `ce-viknorg` / `ce-viknlink` / `ce-viknactor`), `Actor`, subject naming, `BusConfigured()`, `Connect`, `EnsureStream` |
| `outbox` | `Event` row (`integration_outboxes`), `Enqueue` gated on `NATS_URL`, `Dispatcher`, `NATSPublisher`, `Run` |
| `consumer` | `Subscribe` + `Handle` harness: parse, integration-actor loop breaker, backoff nak, poison → `integration_event_rejections`, then terminate |
| `link` | `Link` row (`integration_links`): this local record ↔ that remote record, with the cached far-side status |

## Rules the harness enforces

- **An event whose actor is an integration client never fires a tenant
  rule.** `consumer.Options.AllowIntegrationActor` is false by default; only
  pure observers (the audit consumer) turn it on.
- **Nothing is enqueued without a broker.** `outbox.Enqueue` is a no-op when
  `NATS_URL` is unset, so a table cannot grow with rows nothing drains.
- **A message that fails `MaxDeliver` times is recorded, then terminated.**
  It lands in the app's own `integration_event_rejections` table with headers,
  payload and reason, instead of vanishing.
- **`ce-viknorg` is the organization on the wire.** `ParseMsg` falls back to
  `data.organization_id` for one release, and marks the envelope
  `OrganizationFromBody` so consumers can count how often that still happens.

## Environment

| Variable | Meaning |
|---|---|
| `NATS_URL` | broker; unset = bus off for this process |
| `NATS_SUBJECT_PREFIX` | defaults to `vikn` |
| `NATS_STREAM` | defaults to `VIKN` (per-app binaries read it themselves) |

## Tests

```sh
go test ./...                                   # unit tests
CONNECT_TEST_DSN='postgres:///vikn_connect_test?host=/tmp&sslmode=disable' go test ./...   # + Postgres-backed
```

DB-backed tests drop and recreate only the module's own tables.
