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
2. The gateway forwards the request to `payment-command-service` and propagates `X-Correlation-Id`.
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
- Request-level idempotency with `Idempotency-Key`
- Idempotent query projection using processed event tracking
- Event-driven ledger posting with idempotent double-entry records
- Event-driven notification records with processed event tracking
- Contract-first gRPC for internal fraud and limit checks
- API Gateway with REST routing
- Correlation ID propagation
- In-memory gateway rate limiting
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

Start infrastructure:

```bash
docker compose -f platform/docker/docker-compose.yml up -d
```

Run required services:

```bash
mvn -pl services/fraud-detection-service spring-boot:run
mvn -pl services/limit-management-service spring-boot:run
mvn -pl services/payment-command-service spring-boot:run
mvn -pl services/payment-query-service spring-boot:run
mvn -pl services/ledger-service spring-boot:run
mvn -pl services/notification-service spring-boot:run
mvn -pl services/payment-api-gateway spring-boot:run
```

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
