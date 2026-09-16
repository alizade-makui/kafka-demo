# Kafka Orders Demo

A small but complete event-driven order processing demo built with Spring Boot, Apache Kafka, Maven modules, Spring Data JPA, and H2.

The project shows a practical producer/consumer flow:

1. `order-api` receives an HTTP request.
2. It creates an `OrderCreatedEvent`.
3. It publishes the event to Kafka.
4. `order-worker` consumes the event.
5. The worker validates and processes the order.
6. The processed result is stored in H2 and can be queried through REST endpoints.

## Architecture

```text
Client
  |
  | POST /api/orders
  v
order-api
  |
  | OrderCreatedEvent
  v
Kafka topic: orders.created.v1
  |
  | consume
  v
order-worker
  |
  | validate + calculate total amount
  v
H2 database: processed_orders
  |
  | GET /api/processed-orders
  v
Client
```

## Modules

| Module | Responsibility |
| --- | --- |
| `order-contracts` | Shared Kafka event contracts, currently `OrderCreatedEvent`. |
| `order-api` | REST API for accepting order requests and publishing events to Kafka. |
| `order-worker` | Kafka consumer that processes events, stores results, and exposes query endpoints. |

## Tech Stack

- Java 21 target
- Spring Boot 3.5.x
- Spring Web
- Spring Kafka
- Spring Data JPA
- H2 file database
- Apache Kafka via Docker Compose
- Maven multi-module build
- Bean Validation
- Lombok

## Kafka Flow

The API publishes an event to:

```text
orders.created.v1
```

The event key is the `orderId`, which keeps all records for the same order on the same Kafka partition.

Producer settings include:

- `acks=all`
- idempotent producer enabled
- JSON serialization without type headers
- bounded send timeout handling

Consumer settings include:

- consumer group: `order-processing-v1`
- `auto-offset-reset=earliest`
- record-level offset commit after successful listener execution
- JSON deserialization into `OrderCreatedEvent`
- container stop on listener failure

## REST API

### Create Order

```http
POST /api/orders
```

Example request:

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

The API returns `202 Accepted` after Kafka confirms that the event was written. This does not mean the worker has already processed the order. It means the event is safely accepted by Kafka.

### Get Processed Order By ID

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
  "processedAt": "2026-09-16T08:30:00Z"
}
```

### Get Processed Orders With Pagination

```http
GET /api/processed-orders?page=0
```

The page size is fixed at `10` records per page.

Use the next page like this:

```http
GET /api/processed-orders?page=1
```

## Data Model

Processed orders are stored in the `processed_orders` table.

```sql
CREATE TABLE IF NOT EXISTS processed_orders (
    order_id UUID PRIMARY KEY,
    event_id UUID NOT NULL,
    status VARCHAR(30) NOT NULL,
    total_amount DECIMAL(19, 2) NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE NOT NULL
);
```

`order_id` is the primary key. This prevents duplicate rows for the same order if Kafka redelivers a message.

## Local Run

### Prerequisites

- Docker Compose v2
- Maven
- JDK 21

If you only have JDK 17 installed locally, the current code also compiles with Java 17 when running Maven with:

```powershell
'-Djava.version=17'
```

Example:

```powershell
mvn -pl order-worker -am clean compile '-Djava.version=17'
```

### Start Kafka

```powershell
docker compose up -d
```

Kafka is exposed on:

```text
localhost:9094
```

The Docker Compose file disables automatic topic creation. The `order-api` module defines the topic through Spring Kafka admin configuration.

### Run order-api

```powershell
$env:SPRING_KAFKA_BOOTSTRAP_SERVERS='localhost:9094'
mvn -pl order-api -am spring-boot:run
```

The API runs on:

```text
http://localhost:8081
```

### Run order-worker

Open a second terminal:

```powershell
$env:SPRING_KAFKA_BOOTSTRAP_SERVERS='localhost:9094'
mvn -pl order-worker -am spring-boot:run
```

The worker runs on:

```text
http://localhost:8082
```

## Manual End-to-End Test

Send an order:

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

Read the processed result:

```powershell
Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8082/api/processed-orders/$($response.orderId)"
```

Read all processed orders, 10 per page:

```powershell
Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8082/api/processed-orders?page=0"
```

Expected result:

- `order-api` returns `ACCEPTED`.
- `order-worker` logs the consumed order.
- H2 stores the processed order.
- The query endpoint returns `PROCESSED` with the calculated `totalAmount`.

## H2 Console

When `order-worker` is running, open:

```text
http://localhost:8082/h2-console
```

Use:

| Field | Value |
| --- | --- |
| JDBC URL | `jdbc:h2:file:./data/order-worker` |
| User Name | `alizadeh` |
| Password | empty |

Query:

```sql
SELECT * FROM processed_orders;
```

The database is file-backed, so data survives application restarts as long as the same working directory is used.

## Build

Run the full Maven build:

```powershell
mvn clean verify
```

Build only the API and its dependencies:

```powershell
mvn -pl order-api -am clean compile
```

Build only the worker and its dependencies:

```powershell
mvn -pl order-worker -am clean compile
```

## Project Structure

```text
kafka-demo
  order-contracts
    src/main/java/com/example/orders/contract/kafka/events
      OrderCreatedEvent.java

  order-api
    controller
      OrderController.java
    kafka/producer
      OrderPublisher.java
    kafka/config
      OrderTopicConfiguration.java
    service
      OrderService.java
    service/impl
      OrderServiceImpl.java

  order-worker
    kafka/consumer
      OrderListener.java
    controller
      OrderQueryController.java
    model/entity
      ProcessedOrder.java
    repository
      ProcessedOrderRepository.java
    service
      OrderProcessingService.java
    service/impl
      OrderProcessingServiceImpl.java
```

## Why This Project Matters

This repository demonstrates the core building blocks of an event-driven service:

- separating command intake from background processing
- sharing event contracts through a dedicated module
- publishing to Kafka with explicit confirmation handling
- consuming events with a dedicated worker service
- persisting processed state with JPA
- exposing query endpoints for processed data
- handling duplicate delivery with a database primary key
- keeping the codebase small enough to understand end to end

It is intentionally simple, but it follows the same flow used in larger production systems.

## Current Limitations

- No dead-letter topic yet.
- No retry topic strategy yet.
- No distributed tracing yet.
- No production database profile yet.
- No authentication or authorization.
- The API stores only Kafka acceptance metadata; processed state belongs to the worker.

These are good next steps for evolving the demo into a more production-ready sample.
