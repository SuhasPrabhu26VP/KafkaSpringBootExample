# Kafka Spring Boot Example

A multi-service Spring Boot application demonstrating Apache Kafka producer, Kafka Streams processing, and consumer patterns — with Avro serialization and Confluent Schema Registry.

📄 **Full Documentation:** [suhasprabhu26vp.github.io/-KafkaSpringBootDOC](https://suhasprabhu26vp.github.io/-KafkaSpringBootDOC/#core-concepts)

---

## Tech Stack

| Layer | Technology | Version |
|---|---|---|
| Language | Java | 17 |
| Framework | Spring Boot | 4.0.6 |
| Messaging | Apache Kafka (KRaft) | Confluent Platform 7.8.0 |
| Stream Processing | Kafka Streams | via Spring Kafka |
| Serialization | Apache Avro | 1.12.1 |
| Schema Registry | Confluent Schema Registry | 7.8.0 |
| Avro SerDes | kafka-avro-serializer | 8.2.0 |
| Build Tool | Maven | 3.9.6 |
| Containerization | Docker + Docker Compose | - |
| UI | Kafka UI (provectuslabs) | latest |

---

## Project Structure

```
KafkaSpringBootExample/
├── kafka-producer/
│   └── kafka-producer/        # Spring Boot producer app (port 8083)
├── kafka-consumer/
│   └── kafka-consumer/        # Spring Boot consumer app (port 8084)
├── streams/                   # Kafka Streams processor app (port 8082)
└── kafka-docker-compose/
    └── docker-compose.yml     # Full cluster + apps setup
```

---

## Services

| Service | Port | Description |
|---|---|---|
| kafka1 | 9092 | Kafka broker 1 (KRaft) |
| kafka2 | 9094 | Kafka broker 2 (KRaft) |
| kafka3 | 9096 | Kafka broker 3 (KRaft) |
| schema-registry | 8081 | Confluent Schema Registry |
| kafka-ui | 8080 | Kafka UI dashboard |
| kafka-producer | 8083 | Produces Avro messages |
| streams | 8082 | Kafka Streams processor |
| kafka-consumer | 8084 | Consumes and processes messages |

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and **running**
- Git

---

## Build & Run

### 1. Clone the repository

```bash
git clone https://github.com/SuhasPrabhu26VP/KafkaSpringBootExample.git
cd KafkaSpringBootExample/kafka-docker-compose
```

### 2. Start everything

```bash
docker compose up --build
```

This single command will:
- Pull Kafka, Schema Registry, and Kafka UI images
- Build all three Spring Boot apps inside Docker using multistage Maven builds (no local Maven/Java needed)
- Start the 3-broker KRaft Kafka cluster
- Start Schema Registry and Kafka UI
- Start the producer, streams, and consumer apps in the correct order

> **First run takes 8–15 minutes** — Maven downloads all dependencies into Docker layer cache. Subsequent runs are fast.

### 3. Verify services are up

```bash
docker compose ps
```

Or open **Kafka UI** at [http://localhost:8080](http://localhost:8080)

---

## Startup Order

The services start in a strict dependency chain:

```
kafka1 + kafka2 + kafka3 (healthy)
        ↓
  schema-registry (healthy)
        ↓
  kafka-ui        kafka-producer (healthy)
                        ↓
                  kafka-streams (started)
                        ↓
                  kafka-consumer
```

---

## Testing with Postman

Import `kafka.json` from the root of the repo into Postman to test the producer endpoints.

Producer base URL: `http://localhost:8083`

---

## Stopping

```bash
# Stop all containers
docker compose down

# Stop and remove all data volumes (clean slate)
docker compose down -v
```

---

## Rebuilding a Single App

If you change code in only one service:

```bash
docker compose up --build kafka-producer
```
