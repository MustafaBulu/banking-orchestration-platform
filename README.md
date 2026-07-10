# Payment Orchestration Platform

A backend architecture showcase project for a banking-tech Software Engineer role.

The project demonstrates a production-style payment processing platform using Java, Spring Boot,
microservices, Hexagonal Architecture, DDD, CQRS, Event Sourcing, Kafka, gRPC, PostgreSQL, and
resiliency patterns.

The design goal is not breadth for its own sake: the money spine (gateway → command → outbox →
relay → ledger, plus idempotency and limit reservation/compensation) is treated as a **proven
core** and backed by Testcontainers integration tests that fail if the property they assert is
broken. Fraud remains a **supporting service** with lighter coverage. See
[Scope and Coverage](#scope-and-coverage) for the exact split and
[What is proven (and by which test)](#what-is-proven-and-by-which-test) for the evidence.

## Architecture

The platform is organized as a Maven monorepo with independent Spring Boot services.

- `payment-api-gateway`: external REST entrypoint, routing, correlation ID propagation, rate limiting, timeout mapping.
- `payment-command-service`: write side for payment commands, domain aggregate, event store, outbox, Kafka relay.
- `payment-query-service`: read side for payment projections and query APIs.
- `fraud-detection-service`: synchronous fraud evaluation over gRPC.
- `limit-management-service`: synchronous limit reservation over gRPC, backed by persistent reservation leases.
- `ledger-service`: event-driven ledger posting with double-entry accounting records.
- `notification-service`: event-driven customer notification record creation.
- `libs/event-contracts`: versioned JSON Schemas, shared event envelopes, and the in-process event schema registry.
- `libs/contracts-grpc`: generated Java gRPC contracts from protobuf files.
- `libs/common-domain`: shared domain abstractions.

## Payment Flow

1. A client sends `POST /v1/payments` to `payment-api-gateway`.
2. The gateway forwards the request to `payment-command-service` and propagates `X-Correlation-Id` and `Idempotency-Key`.
3. `payment-command-service` performs synchronous checks **before opening any database transaction**,
   so no connection is held across a remote call:
   - fraud check via `fraud-detection-service` gRPC
   - limit reservation via `limit-management-service` gRPC
4. If checks pass, the payment aggregate emits a `PaymentCreated` domain event.
5. A single database transaction writes the event to PostgreSQL `event_store` and the outbox record
   to PostgreSQL `outbox`.
6. If that transaction fails after a limit was reserved, the command service issues a compensating
   `Release` to `limit-management-service`. If that release is not delivered because the process dies,
   the reservation is still bounded by the limit service's persistent lease expiry.
7. The outbox relay publishes the event to Kafka.
8. `payment-query-service` consumes the Kafka event and updates `payment_overview`.
9. `ledger-service` consumes the same Kafka event and writes balanced debit/credit ledger entries.
10. `notification-service` consumes the event and records a customer notification intent.
11. A client can query `GET /v1/payments/{paymentId}` through the gateway.

The read side is eventually consistent by design.

Every consumer (ledger, notification, query) runs behind a Spring Kafka `DefaultErrorHandler` with a
bounded `FixedBackOff` and a `DeadLetterPublishingRecoverer`. A message that keeps failing is retried
a fixed number of times (`app.kafka.retry.max-attempts` / `app.kafka.retry.backoff-ms`) and then
routed to `<topic>.DLT` instead of hot-looping or being silently dropped, so one poison message never
blocks its partition. This mirrors, on the consumer side, the bounded retry and dead-letter behaviour
the outbox relay already provides on the producer side.

## Implemented Capabilities

- Java 21 + Spring Boot + Maven
- Microservice-oriented monorepo
- Hexagonal Architecture with DDD tactical patterns
- CQRS with separate command and query services
- Event Sourcing foundation on the payment write side
- PostgreSQL event store
- Transactional outbox pattern
- Kafka-based asynchronous event publishing
- Versioned JSON Schema contracts for Kafka domain events
- Required request-level idempotency with `Idempotency-Key` for payment creation
- Persistent limit reservations with release and lease-expiry cleanup
- Idempotent query projection using processed event tracking
- Event-driven ledger posting with idempotent double-entry records
- Event-driven notification records with processed event tracking
- Contract-first gRPC for internal fraud and limit checks
- API Gateway with REST routing
- Correlation ID propagation
- In-memory gateway rate limiting
- Spring Actuator health and Prometheus metrics endpoints
- Prometheus, Grafana, OpenTelemetry Collector, Tempo, Loki, and Promtail based local observability foundation
- Distributed tracing (Micrometer Tracing over the OpenTelemetry SDK) with W3C `traceparent`
  propagation across the gateway → command HTTP hop and the relay → consumer Kafka hop
- Provisioned Grafana dashboard for service health, payment outcomes, outbox, downstream processing, and logs
- Domain-level metrics for payments, idempotency, outbox publishing, ledger posting, and notifications
- Service Dockerfiles for container image builds
- Minimal Kubernetes deployment manifests with Service-based discovery and load balancing
- GitHub Actions CI for Maven build, tests, Docker image builds, and deployment manifest validation
- Resiliency patterns:
  - timeout
  - retry
  - circuit breaker
  - bulkhead
  - outbox retry/backoff
  - consumer bounded retry with dead-letter topic (DLT)
- Docker Compose based local infrastructure

## Contracts

gRPC protobuf contracts are stored in `contracts/proto`.

- `fraud-control.proto` — `Evaluate`
- `limit-control.proto` — `Reserve` and `Release` (the compensating step for a rolled-back write)

The `libs/contracts-grpc` module generates Java stubs from these contracts.

Kafka domain event contracts live in `libs/event-contracts`.

- `schemas/payment/domain-events/payment-created-v1.schema.json` defines the `PaymentCreated` v1
  envelope and payload.
- `PaymentCreatedEventEnvelope` and `PaymentCreatedPayload` are shared by the producer and
  consumers instead of each service carrying its own hand-rolled envelope record.
- `EventSchemaRegistry` is the in-process registry used by the command service before inserting an
  outbox row and by ledger/query/notification consumers before deserializing a Kafka message.
- `PaymentCreatedSchemaContractTest` compares the current schema to the checked-in v1 baseline and
  fails if a required field, property, or scalar constraint is removed or changed incompatibly.

## Consistency and Compensation

Creating a payment spans a remote side effect (the limit reservation) and a local database write, so
it is a small saga rather than a single atomic action. The command service is the coordinator:

1. The fraud and limit gRPC checks run **outside** any database transaction. This keeps remote calls
   off the connection pool — a database connection is never held open across an RPC.
2. The `event_store` and `outbox` writes then happen in **one** local transaction (the outbox is
   atomic with the event).
3. If that transaction fails after a reservation was taken, the coordinator issues a compensating
   `Release`.
4. Limit reservations are persisted with a lease. A scheduled reaper marks expired active reservations
   `EXPIRED`, so a missed compensating release is bounded by the configured lease duration instead of
   being left in process memory forever.

Request-level idempotency follows the same "no RPC inside a transaction" rule. The `Idempotency-Key`
is claimed in a short transaction (`INSERT ... ON CONFLICT DO NOTHING` as the ownership gate), the
gRPC checks and persistence run with no transaction held, and the stored response is written at the
end. A concurrent request that loses the claim race is rejected with `409 Conflict`
(`IDEMPOTENCY_IN_PROGRESS`) instead of double-executing.

**Known residual window (documented, not hidden):** the compensating `Release` call is still
best-effort. If the command service process dies *after* reserving but *before* the release is
delivered, the reservation remains active only until the limit service lease expires and the reaper
marks it `EXPIRED`. This is not the same as a durable compensation log with replay, but it closes the
unbounded orphaned-reservation window. The `limit.reservation.release.failed` metric counts
compensations that could not be delivered, and `limit.reservation.expired` counts lease expirations.

## Local Runtime

Build the service JARs and start the full Docker runtime:

```bash
mvn clean package
docker compose -f platform/docker/docker-compose.yml up -d --build
```

The stack starts PostgreSQL, Kafka, observability components, and all application services.
The API gateway is exposed at `http://localhost:8080`.

For IDE debugging, individual services can still be started with Maven against the same local
PostgreSQL and Kafka dependencies.

Create a payment:

```http
POST http://localhost:8080/v1/payments
Content-Type: application/json
Idempotency-Key: demo-payment-001

{
  "customerId": "cust-1001",
  "amount": 120.50,
  "currency": "EUR"
}
```

Query the payment after a short delay:

```http
GET http://localhost:8080/v1/payments/{paymentId}
```

## Container Images

Each Spring Boot service includes a Dockerfile that packages the built JAR into a Java 21 runtime image.

Example:

```bash
mvn clean package
docker build -f services/payment-api-gateway/Dockerfile -t payment-api-gateway:local services/payment-api-gateway
```

## Kubernetes

Minimal deployment manifests are stored in `platform/kubernetes/base`.

```bash
kubectl apply -k platform/kubernetes/base
```

The Kubernetes setup uses native `Service` objects for internal service discovery and load balancing.
For example, `payment-api-gateway` calls `payment-command-service` through the stable
`http://payment-command-service:8081` DNS name. This keeps discovery Kubernetes-native instead of
adding a separate registry such as Eureka.

The included `Secret` manifest uses demo local credentials only; production deployments should replace
it with externally managed secrets.

## Observability

Each service exposes operational endpoints through Spring Actuator:

```http
GET /actuator/health
GET /actuator/prometheus
```

Prometheus, Grafana, OpenTelemetry Collector, Tempo, Loki, and Promtail are included in the local Docker runtime:

```text
Prometheus:              http://localhost:9090
Grafana:                 http://localhost:3000
Tempo:                   http://localhost:3200
Loki:                    http://localhost:3100
OpenTelemetry Collector: http://localhost:4318/v1/traces
```

Grafana is provisioned with Prometheus, Tempo, and Loki datasources plus a `Payment Platform Overview`
dashboard. The local Prometheus configuration scrapes service containers through Docker service DNS.
Services export traces to the OpenTelemetry Collector over OTLP, and the collector forwards them to
Tempo. Application logs are written to local `logs/*.log` files and Promtail forwards them to Loki.
Logs include `traceId` and `spanId` fields for trace-log correlation.

### Distributed tracing

Tracing is wired with Spring Boot's OpenTelemetry modules (Micrometer Tracing over the OpenTelemetry
SDK) with W3C `traceparent` propagation and OTLP export to the collector. Propagation is enabled
across the payment spine:

- **HTTP (gateway → command):** the gateway's `WebClient` is built from the auto-configured,
  observation-instrumented builder, so the outgoing call carries `traceparent` and the command
  service continues the same trace.
- **gRPC (command → fraud/limit):** command-service channels use a client interceptor that injects
  the current span context into gRPC metadata. Fraud and limit servers use server interceptors that
  extract that metadata and run RPC handling inside a server span with the caller's trace id.
- **Outbox boundary (command → relay):** the command service persists a nullable W3C `traceparent`
  on the `outbox` row when the event is enqueued. The relay validates that value, restores it as the
  parent for an `outbox.relay` producer span, and publishes with trace context in Kafka headers. The
  atomic event-store + outbox write remains one local transaction.
- **Kafka (relay → consumers):** the outbox relay's `KafkaTemplate` is observation-enabled, so it
  injects `traceparent` into the record headers; the ledger, notification, and query listener
  containers are observation-enabled, so each consumer continues the trace carried in the headers.

Automated proof covers both sides of the asynchronous boundary:

- `GrpcClientTracePropagationTest` (payment-command-service) proves the command-side gRPC client
  interceptor injects a W3C `traceparent` metadata value carrying the active span's trace id.
- `FraudGrpcTracePropagationTest` and `LimitGrpcTracePropagationTest` prove the fraud and limit gRPC
  server interceptors continue the same trace id from incoming gRPC metadata.
- `KafkaTracePropagationIntegrationTest` (payment-command-service) creates a payment inside an
  active request span, asserts the outbox row stores that span's trace id, runs the relay, and
  asserts the relay-published Kafka record carries the same trace id in a W3C `traceparent` header.
- `LedgerTracePropagationIntegrationTest` (ledger-service) publishes a real Kafka record with a W3C
  `traceparent` header and asserts the ledger listener observes an active span with that same trace
  id.

Tempo remains wired in the local Docker runtime for visual inspection through Grafana; the regression
guard is the automated Testcontainers proof above.

Selected domain metrics:

- `payments_created_total`
- `payments_rejected_total`
- `idempotency_hit_total`
- `limit_reservation_compensated_total`
- `limit_reservation_release_failed_total`
- `limit_reservation_expired_total`
- `outbox_pending`
- `outbox_published_total`
- `outbox_publish_failed_total`
- `ledger_entries_posted_total`
- `notifications_recorded_total`

## Testing

The suite has two layers: fast unit tests, and Testcontainers-based integration tests that prove
the core correctness claims against real PostgreSQL and Kafka.

Run the full build (unit + integration):

```bash
mvn clean package
```

Run command-service tests with dependent modules:

```bash
mvn -pl services/payment-command-service -am test
```

The integration tests start real PostgreSQL and Kafka containers via Testcontainers. They run under
`mvn test` when Docker is available and skip cleanly when it is not
(`@Testcontainers(disabledWithoutDocker = true)`), so the build stays green on machines without
Docker while executing on Docker-equipped CI runners.

### What is proven (and by which test)

- Outbox atomicity and relay — `OutboxAtomicityAndRelayIntegrationTest` (payment-command-service):
  creating a payment writes the `event_store` row and the `outbox` row, the relay publishes the
  event to Kafka and marks the outbox row `PUBLISHED`, and a real Kafka consumer receives the event
  keyed by its `eventId`.
- Consumer idempotency under real duplicate delivery — `LedgerConsumerIdempotencyIntegrationTest`
  (ledger-service): delivering the same `PaymentCreated` event twice results in exactly one ledger
  posting — two balanced entries and a single processed-event record.
- Query projection idempotency under real duplicate delivery —
  `PaymentProjectionConsumerIdempotencyIntegrationTest` (payment-query-service): delivering the same
  `PaymentCreated` event twice results in exactly one `payment_overview` row and one processed-event
  record.
- Notification idempotency under real duplicate delivery —
  `PaymentNotificationConsumerIdempotencyIntegrationTest` (notification-service): delivering the same
  `PaymentCreated` event twice records exactly one notification and one processed-event record.
- Required idempotency-key replay — `IdempotencyKeyReplayIntegrationTest` (payment-command-service):
  the same `Idempotency-Key` with the same body creates one payment and replays the stored response,
  while the same key with a different body returns `409 Conflict`.
- Limit reservation is compensated on failure — `ReservationCompensationIntegrationTest`
  (payment-command-service): when persistence fails after the limit was reserved, the command service
  releases the reservation and leaves no `event_store` or `outbox` row behind.
- Limit reservations are durable and lease-bounded — `LimitReservationIntegrationTest`
  (limit-management-service): reservations are persisted in PostgreSQL, duplicate reserve calls for
  the same payment reuse the active reservation, and an undelivered release is cleaned up by lease
  expiry.
- Remote checks never run inside a database transaction — `GrpcOutsideTransactionIntegrationTest`
  (payment-command-service): during the fraud and limit gRPC calls, no transaction is active, so a DB
  connection is never held across an RPC.
- A poison message is dead-lettered instead of looping forever — `PoisonMessageDeadLetterIntegrationTest`
  (ledger-service): a message that always fails to parse is retried a bounded number of times, then
  published to `payment.domain.events.DLT`, and the consumer continues with the next valid event
  (the partition is not blocked).
- Trace context crosses the outbox and Kafka consumer boundary —
  `KafkaTracePropagationIntegrationTest` (payment-command-service) proves the originating request
  trace id is persisted on the outbox row and carried by the relay-published Kafka record;
  `LedgerTracePropagationIntegrationTest` (ledger-service) proves the ledger listener continues the
  same trace id from Kafka headers.
- Trace context propagates through gRPC checks — `GrpcClientTracePropagationTest`
  (payment-command-service) proves `traceparent` is injected into gRPC metadata, and
  `FraudGrpcTracePropagationTest` / `LimitGrpcTracePropagationTest` prove the fraud and limit server
  spans continue the same trace id from that metadata.
- Kafka event schema compatibility — `PaymentCreatedSchemaContractTest` (event-contracts) proves the
  current `PaymentCreated` v1 JSON Schema still matches its baseline contract and rejects an
  incompatible payload at build/test time; `OutboxAtomicityAndRelayIntegrationTest`
  (payment-command-service) validates both the persisted outbox payload and the relay-published
  Kafka value through `EventSchemaRegistry`.
- Wiring against real infrastructure — `ContextLoadsIntegrationTest` (command and ledger): each
  service boots against real PostgreSQL (Flyway migrations applied) and Kafka.

These tests assert exact-count invariants (one posting, one payment, one outbox row) rather than
mere presence, so a regression surfaces as a failure — for example, removing the outbox write makes
the outbox atomicity test fail.

### Unit tests

Unit tests cover gRPC resiliency and lower-level behavior:

- timeout fallback
- retry recovery
- circuit breaker fast-fail behavior
- idempotent payment command handling
- balanced double-entry ledger posting
- idempotent notification recording

## CI/CD

GitHub Actions runs the main quality gate on pushes and pull requests:

- Maven build and tests
- Docker Compose configuration validation
- service container image builds
- Kubernetes manifest rendering with Kustomize

## Scope and Coverage

Coverage across the seven services is deliberately uneven. The correctness effort is concentrated on
the money spine and stated honestly rather than spread thin to look uniform.

**Proven core** — deep integration coverage against real PostgreSQL and Kafka
(see [What is proven](#what-is-proven-and-by-which-test)):

- `payment-api-gateway` → `payment-command-service`: request idempotency, the atomic
  event-store + outbox write, and the outbox relay to Kafka.
- `limit-management-service` reservation with compensation on a rolled-back write, backed by
  persistent leases and expiry cleanup.
- `ledger-service`: idempotent double-entry posting under real duplicate delivery, and consumer
  bounded-retry + dead-letter behaviour.
- `payment-query-service`: idempotent read-model projection under real duplicate delivery.
- `notification-service`: idempotent notification recording under real duplicate delivery.
- Trace-context propagation through Kafka on the relay → consumer hop.
- Kafka event contract enforcement for `PaymentCreated` v1 through JSON Schema validation and a
  baseline compatibility test.

**Supporting services** — lighter coverage, exercised mainly by unit tests and context/wiring tests,
not by dedicated end-to-end proofs:

- `fraud-detection-service`: synchronous gRPC fraud check (gRPC resiliency is unit-tested on the
  client adapter side).

This split is intentional: the flow that moves money and must not double-post or lose events is the
part that is proven; fraud scoring is demonstrated but not exhaustively verified.
