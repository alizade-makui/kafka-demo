# Kafka Orders Demo

A compact event-driven order-processing demo built with **Java 21**, **Spring Boot**, **Spring Kafka**, **Spring Data JPA**, **H2**, and **Apache Kafka (KRaft)**.

The project is intentionally small enough to understand end to end, while still demonstrating the Kafka concepts that matter in a real backend service: keys, partitions, consumer groups, offset handling, idempotent processing, retries, and a Dead Letter Topic (DLT).

## Architecture

```text
                         POST /api/orders
                               |
                               v
                        +-------------+
                        |  order-api  |
                        +------+------+ 
                               |
                               | OrderCreatedEvent
                               | key = orderId
                               v
                    +-----------------------+
                    | orders.created.v1     |
                    | P0      P1      P2    |
                    +-----------+-----------+
                                |
                                | group: order-processing-v1
                                v
                         +--------------+
                         | order-worker |
                         +------+-------+
                                |
                    +-----------+-----------+
                    |                       |
                 success                  failure
                    |                       |
                    v                       | retry x2
             processed_orders               |
                    |                       v
                    |              orders.created.v1.dlt
                    v
       GET /api/processed-orders/**
```

## Modules

| Module | Responsibility |
| --- | --- |
| `order-contracts` | Shared Kafka contract containing `OrderCreatedEvent`. |
| `order-api` | Accepts HTTP order requests, creates events, and publishes them to Kafka. |
| `order-worker` | Consumes events, validates/processes them, persists results, and exposes query endpoints. |

## Tech Stack

- Java 21
- Spring Boot 3.5.x
- Spring Web
- Spring Kafka
- Spring Data JPA / Hibernate
- Bean Validation
- H2 file database
- Apache Kafka with KRaft via Docker Compose
- Maven multi-module build
- Lombok

## Kafka Design

### Main topic

```text
orders.created.v1
```

The topic is declared with:

```text
partitions = 3
replication-factor = 1
```

Replication factor `1` is intentional for this single-broker local demo.

### Message key

`orderId` is used as the Kafka key:

```java
kafkaTemplate.send(topic, event.orderId().toString(), event);
```

This keeps records with the same order key on the same partition and preserves ordering for that key while allowing different orders to be processed in parallel.

### Producer reliability

The producer uses:

```text
acks=all
enable.idempotence=true
```

The API waits for Kafka acknowledgement before returning `202 Accepted`, and includes the resulting partition and offset in the response.

### Consumer group and concurrency

The worker uses:

```text
group-id = order-processing-v1
concurrency = 3
```

With three partitions, up to three consumers in this group can actively process partitions in parallel.

### Offset handling

```text
enable-auto-commit=false
ack-mode=record
```

The listener only advances successfully handled records through Spring Kafka's container-managed acknowledgement flow.

The design therefore follows an **at-least-once** processing model: a record can be delivered again after a failure, so the consumer must tolerate duplicate delivery.

### Idempotent processing

Processed orders use `order_id` as the database primary key:

```sql
CREATE TABLE IF NOT EXISTS processed_orders (
    order_id UUID PRIMARY KEY,
    event_id UUID NOT NULL,
    status VARCHAR(30) NOT NULL,
    total_amount DECIMAL(19, 2) NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE NOT NULL
);
```

Before creating a row, the worker checks whether the order has already been processed. The primary key remains the final database integrity constraint.

## Retry and Dead Letter Topic

Failed listener processing is handled by Spring Kafka's `DefaultErrorHandler` and `DeadLetterPublishingRecoverer`.

Retryable failures follow this flow:

```text
original attempt
      |
      X
      |
   wait 1s
      |
   retry #1
      |
      X
      |
   wait 1s
      |
   retry #2
      |
      X
      v
orders.created.v1.dlt
```

The current backoff is:

```java
new FixedBackOff(1_000L, 2L)
```

So there are **3 total attempts**: the original attempt plus two retries.

`InvalidOrderEventException` is configured as non-retryable because retrying invalid business data will not make it valid:

```text
InvalidOrderEventException -> DLT directly
```

The DLT is:

```text
orders.created.v1.dlt
```

It also has three partitions so a failed record can be published to the same partition number as the source record.

## Event Contract

`OrderCreatedEvent` currently contains:

```text
eventId
schemaVersion
occurredAt
orderId
customerId
productCode
quantity
unitPrice
```

The demo uses JSON serialization and explicitly targets `OrderCreatedEvent` on the consumer side. Type headers are not required for the main topic.

`schemaVersion` is currently `1`; breaking event changes should be introduced deliberately instead of silently changing the existing contract.

## REST API

### Create an order

```http
POST /api/orders
```

Example body:

```json
{
  "customerId": "customer-1",
  "productCode": "BOOK-001",
  "quantity": 2,
  "unitPrice": 150000
}
```

Example response:

```json
{
  "orderId": "8d9be6a0-dc1d-4b95-bb77-458e2c67801f",
  "eventId": "41c33b35-bcd1-4a5d-9b5d-bb21b967951e",
  "status": "ACCEPTED",
  "partition": 1,
  "offset": 42
}
```

`202 Accepted` means Kafka acknowledged the event. It does **not** mean `order-worker` has already finished processing it.

### Get a processed order

```http
GET /api/processed-orders/{orderId}
```

Example response:

```json
{
  "orderId": "8d9be6a0-dc1d-4b95-bb77-458e2c67801f",
  "eventId": "41c33b35-bcd1-4a5d-9b5d-bb21b967951e",
  "status": "PROCESSED",
  "totalAmount": 300000,
  "processedAt": "2026-09-26T08:30:00Z"
}
```

### List processed orders

```http
GET /api/processed-orders?page=0
```

The current page size is fixed at `10`.

## Running Locally

### Prerequisites

- JDK 21
- Maven
- Docker + Docker Compose v2

### 1. Start Kafka

```bash
docker compose up -d
```

Kafka is exposed to host applications at:

```text
localhost:9094
```

Automatic topic creation is disabled.

### 2. Start `order-api`

The current project declares `orders.created.v1` from `order-api`, so start the API before the worker on a clean Kafka volume.

PowerShell:

```powershell
$env:SPRING_KAFKA_BOOTSTRAP_SERVERS='localhost:9094'
mvn -pl order-api -am spring-boot:run
```

Bash:

```bash
SPRING_KAFKA_BOOTSTRAP_SERVERS=localhost:9094 \
  mvn -pl order-api -am spring-boot:run
```

API address:

```text
http://localhost:8081
```

### 3. Start `order-worker`

PowerShell:

```powershell
$env:SPRING_KAFKA_BOOTSTRAP_SERVERS='localhost:9094'
mvn -pl order-worker -am spring-boot:run
```

Bash:

```bash
SPRING_KAFKA_BOOTSTRAP_SERVERS=localhost:9094 \
  mvn -pl order-worker -am spring-boot:run
```

Worker address:

```text
http://localhost:8082
```

The worker declares `orders.created.v1.dlt` during startup.

## End-to-End Test

PowerShell:

```powershell
$response = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8081/api/orders" `
  -ContentType "application/json" `
  -Body '{
    "customerId": "customer-1",
    "productCode": "BOOK-001",
    "quantity": 2,
    "unitPrice": 150000
  }'

$response
```

Then query the result:

```powershell
Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8082/api/processed-orders/$($response.orderId)"
```

Expected flow:

```text
HTTP request
    -> order-api returns ACCEPTED
    -> Kafka stores OrderCreatedEvent
    -> order-worker consumes it
    -> processed_orders receives one row
    -> query endpoint returns PROCESSED
```

## Kafka CLI: Useful Checks

The Docker Compose service is currently named `Kafka`.

### List topics

```bash
docker compose exec Kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --list
```

Expected topics after both applications have started:

```text
orders.created.v1
orders.created.v1.dlt
```

### Describe the main topic

```bash
docker compose exec Kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --describe \
  --topic orders.created.v1
```

### Inspect consumer lag

```bash
docker compose exec Kafka kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --describe \
  --group order-processing-v1
```

Important columns:

```text
CURRENT-OFFSET
LOG-END-OFFSET
LAG
```

### Read records from the DLT

```bash
docker compose exec Kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic orders.created.v1.dlt \
  --from-beginning
```

## H2 Console

While `order-worker` is running:

```text
http://localhost:8082/h2-console
```

Current connection values:

| Field | Value |
| --- | --- |
| JDBC URL | `jdbc:h2:file:./data/order-worker` |
| User Name | `alizadeh` |
| Password | empty |

Example query:

```sql
SELECT * FROM processed_orders;
```

## Build

Build all modules:

```bash
mvn clean verify
```

Build the API and required modules:

```bash
mvn -pl order-api -am clean verify
```

Build the worker and required modules:

```bash
mvn -pl order-worker -am clean verify
```

## Project Structure

```text
kafka-demo/
├── docker-compose.yaml
├── pom.xml
├── order-contracts/
│   └── src/main/java/com/example/orders/contract/kafka/events/
│       └── OrderCreatedEvent.java
├── order-api/
│   ├── controller/
│   ├── exception/
│   ├── kafka/
│   │   ├── config/
│   │   └── producer/
│   ├── model/
│   └── service/
└── order-worker/
    ├── controller/
    ├── exception/
    ├── kafka/
    │   ├── config/
    │   └── consumer/
    ├── model/
    ├── repository/
    └── service/
```

## Concepts Demonstrated

This repository demonstrates:

- Kafka producer and consumer flow
- topics and partitions
- message keys and per-key ordering
- consumer groups and parallel consumption
- offsets and record-level acknowledgement
- at-least-once processing
- idempotent producer configuration
- idempotent consumer behavior backed by a database primary key
- JSON serialization/deserialization
- retry with fixed backoff
- non-retryable exceptions
- Dead Letter Topic handling
- consumer lag inspection
- KRaft-based local Kafka setup

## Current Scope and Known Limitations

This is a learning/portfolio demo, not a production Kafka platform.

Currently out of scope:

- request-level idempotency for repeated HTTP submissions
- automated tests / Testcontainers integration
- Kafka UI
- Schema Registry / Avro / Protobuf
- Kafka transactions / exactly-once stream processing
- Transactional Outbox
- multi-broker production replication
- security (SASL/SSL/ACLs)
- production database and migrations
- distributed tracing and production observability

These are intentionally left for a later advanced phase so the core Kafka behavior remains easy to understand.
