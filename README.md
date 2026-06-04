# Payment Orchestration Platform

A backend architecture showcase project for a banking-tech Software Engineer role.

The project demonstrates a production-style payment processing platform using Java, Spring Boot,
microservices, Hexagonal Architecture, DDD, CQRS, Event Sourcing, Kafka, gRPC, PostgreSQL, and
resiliency patterns.

## Architecture

The platform is organized as a Maven monorepo with independent Spring Boot services.

- `payment-api-gateway`: external REST entrypoint, routing, correlation ID propagation, rate limiting, timeout mapping.
- `payment-command-service`: write side for payment commands, domain aggregate, event store, outbox, Kafka relay.
- `payment-query-service`: read side for payment projections and query APIs.
- `fraud-detection-service`: synchronous fraud evaluation over gRPC.
- `limit-management-service`: synchronous limit reservation over gRPC.
- `ledger-service`: event-driven ledger posting with double-entry accounting records.
- `notification-service`: event-driven customer notification record creation.
- `libs/contracts-grpc`: generated Java gRPC contracts from protobuf files.
- `libs/common-domain`: shared domain abstractions.

## Payment Flow

1. A client sends `POST /v1/payments` to `payment-api-gateway`.
2. The gateway forwards the request to `payment-command-service` and propagates `X-Correlation-Id` and `Idempotency-Key`.
3. `payment-command-service` performs synchronous checks:
   - fraud check via `fraud-detection-service` gRPC
   - limit reservation via `limit-management-service` gRPC
4. If checks pass, the payment aggregate emits a `PaymentCreated` domain event.
5. The event is written to PostgreSQL `event_store`.
6. The same transaction writes an outbox record to PostgreSQL `outbox`.
7. The outbox relay publishes the event to Kafka.
8. `payment-query-service` consumes the Kafka event and updates `payment_overview`.
9. `ledger-service` consumes the same Kafka event and writes balanced debit/credit ledger entries.
10. `notification-service` consumes the event and records a customer notification intent.
11. A client can query `GET /v1/payments/{paymentId}` through the gateway.

The read side is eventually consistent by design.

## Implemented Capabilities

- Java 21 + Spring Boot + Maven
- Microservice-oriented monorepo
- Hexagonal Architecture with DDD tactical patterns
- CQRS with separate command and query services
- Event Sourcing foundation on the payment write side
- PostgreSQL event store
- Transactional outbox pattern
- Kafka-based asynchronous event publishing
- Required request-level idempotency with `Idempotency-Key` for payment creation
- Idempotent query projection using processed event tracking
- Event-driven ledger posting with idempotent double-entry records
- Event-driven notification records with processed event tracking
- Contract-first gRPC for internal fraud and limit checks
- API Gateway with REST routing
- Correlation ID propagation
- In-memory gateway rate limiting
- Spring Actuator health and Prometheus metrics endpoints
- Prometheus, Grafana, OpenTelemetry Collector, Tempo, Loki, and Promtail based local observability foundation
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
- Docker Compose based local infrastructure

## Contracts

gRPC protobuf contracts are stored in `contracts/proto`.

- `fraud-control.proto`
- `limit-control.proto`

The `libs/contracts-grpc` module generates Java stubs from these contracts.

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

Selected domain metrics:

- `payments_created_total`
- `payments_rejected_total`
- `idempotency_hit_total`
- `outbox_pending`
- `outbox_published_total`
- `outbox_publish_failed_total`
- `ledger_entries_posted_total`
- `notifications_recorded_total`

## Testing

Run the full build:

```bash
mvn clean package
```

Run command-service tests with dependent modules:

```bash
mvn -pl services/payment-command-service -am test
```

Current focused tests cover gRPC resiliency, request idempotency, ledger posting, and notification recording behavior:

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

## Current Scope

This repository currently focuses on the core payment orchestration flow:

- command handling
- fraud and limit checks
- event persistence
- reliable event publishing
- read projection
- ledger posting
- notification recording
- gateway access
- baseline resiliency
