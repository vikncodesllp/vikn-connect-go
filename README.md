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
| `rules` | `Rule` row (`integration_rules`, generalized: source app, source event type, action key, JSON config), `Action` interface + `Registry`, save-time `Validate` with per-field errors, and the `Router` that runs matching rules per delivery and keeps history |
| `delivery` | `Record` row (`integration_deliveries`): one row per (event, action) — idempotency and operator history |
| `token` | `Verifier` for the RS256 integration tokens auth_go mints (JWKS cache, audience + scope checks), and `Client` for obtaining them: client-credentials token, `/api/connect/resolve`, and `GetJSON` against the far side's scoped API |

## Rules the harness enforces

- **An event whose actor is an integration client never fires a tenant
  rule.** `consumer.Options.AllowIntegrationActor` is false by default; only
  pure observers (the audit consumer) turn it on.
- **Nothing is enqueued without a broker.** `outbox.Enqueue` is a no-op when
  `NATS_URL` is unset, so a table cannot grow with rows nothing drains.
- **A new durable starts at new events.** `consumer.Subscribe` creates the
  durable itself and binds to it, so a clean shutdown never deletes it and a
  restart continues where it left off. Only a consumer that sets
  `ReplayHistory` (the audit, link mirroring) reads the stream from the start;
  a rules consumer never runs today's rules over last month's events.
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
| `SSO_JWKS_URL` / `AUTH_SERVICE_URL` | where `token.VerifierFromEnv` fetches keys (sandbox and production share a kid with different keys, so point at your own auth_go) |
| `CONNECT_CLIENT_ID` / `CONNECT_CLIENT_SECRET` | this app's confidential OAuth client; with `AUTH_SERVICE_URL` they make `token.NewClientFromEnv` non-nil |

## Tests

```sh
go test ./...                                   # unit tests
CONNECT_TEST_DSN='postgres:///vikn_connect_test?host=/tmp&sslmode=disable' go test ./...   # + Postgres-backed
```

DB-backed tests drop and recreate only the module's own tables. The consumer
harness also has a real-broker test, opt-in the same way:

```sh
nats-server -js -p 42224 &
CONNECT_TEST_NATS_URL=nats://127.0.0.1:42224 go test ./consumer/ -run Broker -v
```

It publishes three events into a throwaway stream and checks that a user
event reaches the handler once, an integration-actor event is skipped without
touching it, and a message that always fails is delivered `MaxDeliver` times,
recorded as a rejection, then terminated with the consumer fully drained.
