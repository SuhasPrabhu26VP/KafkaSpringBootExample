# Kafka Spring Boot Example
Kafka SpringBoot Simple Example

Test with below API 


POST http://localhost:8083/api/v1/user

{
  "firstName": "Suhas",
  "lastName": "Prabhu",
  "age": 28,
  "active": true,
  "userId": "USER-001",
  "companyId": "COMP-001",
  "department": "ENGINEERING",
  "country": "IN",
  "salary": 2.00,
  "status": "ACTIVE",
  "createdAt": 1717000000000
}


POST http://localhost:8083/api/v1/company
{
  "name": "Kafka Solutions",
  "address": "42 Manyata Park, Bangalore",
  "employeeCount": 500,
  "softwareCompany": true,
  "companyId": "COMP-002",
  "industry": "TECH",
  "country": "IN",
  "revenue": 15000000.00,
  "updatedAt": 1717000000000
}


# Kafka Architecture and Core Concepts

## Overview

This project demonstrates Apache Kafka core concepts with practical implementations for SAP Commerce payment processing. It includes producer and consumer applications with Avro schemas, Schema Registry, Dead Letter Queue (DLQ), and transactional support.

---

## Core Concepts (18 Total)

### 1. PRODUCER
An application that publishes events to Kafka topics, sending records to specific topics with optional keys for partitioning.

**Example:** PayPal acts as producer; after collecting user credentials, it initiates the transaction and sends critical transaction details to SAP Commerce for later confirmation or authentication over time.

---

### 2. CONSUMER
An application that reads events from Kafka topics, processing records at its own pace with control over offset commits.

**Example:** SAP Commerce Order Service consumes payment events; it reads the PayPal transaction details and updates order status without rushing or blocking the customer.

---

### 3. TOPIC
A logical category that organises related events, similar to a database table or folder in a filesystem.

**Example:** "payment-transactions" topic stores every payment event from PayPal or other payment providers.

---

### 4. PARTITION
A physically ordered, immutable sequence of events within a topic that enables parallel processing and horizontal scaling.

**Example:** PayPal transactions go to partition 0, Stripe to partition 1, credit cards to partition 2 so SAP Commerce can process all payment types simultaneously.

---

### 5. OFFSET
A unique sequential ID for each event within a partition that allows consumers to track their reading position and resume from failures.

**Example:** Offset 45 means SAP Commerce has successfully processed 45 PayPal transactions and will start from transaction 46 after a crash.

---

### 6. CONSUMER GROUP
A set of consumers that work together to read from a topic where each partition is assigned to exactly one consumer in the group.

**Example:** Three SAP Commerce payment processors form a group; each takes different payment types so no transaction gets processed twice.

---

### 7. BROKER
A single Kafka server that stores data, handles client requests, and manages partitions and replicas.

**Example:** Broker 1 stores all payment events from last 7 days and serves them to SAP Commerce whenever needed for reconciliation.

---

### 8. CLUSTER
A group of Kafka brokers working together for scalability, fault tolerance, and high availability.

**Example:** Three brokers running together; if one catches fire during Black Friday, the other two continue serving payment events to SAP Commerce.

---

### 9. LEADER & FOLLOWER
For each partition, one broker acts as leader handling all reads and writes while followers replicate data for fault tolerance.

**Example:** Broker 1 leads partition 0 for PayPal payments; Broker 2 and 3 copy all data; if Broker 1 goes down, Broker 2 becomes leader instantly.

---

### 10. ISR (IN-SYNC REPLICAS)
The set of replicas that are fully caught up with the leader, ensuring data durability and consistency guarantees.

**Example:** Two followers have copied the latest PayPal transaction; one slow follower is behind; the ISR count is 2, so data is still safe.

---

### 11. RECORD BATCH
A group of events sent together from producer to broker to improve network efficiency and storage throughput.

**Example:** PayPal sends 1000 transaction details in one network call instead of making 1000 separate calls to Kafka.

---

### 12. RETENTION POLICY
Rules that determine how long events are kept based on time duration or storage size limits.

**Example:** Keep all payment events for 90 days for audit compliance; after that, delete oldest transactions automatically to save disk space.

---

### 13. CONSUMER REBALANCE
The process of reassigning partitions among consumers when a consumer joins, leaves, or fails within a consumer group.

**Example:** One SAP Commerce payment processor crashes; Kafka automatically reassigns its PayPal transactions to the other two processors without manual intervention.

---

### 14. DEAD LETTER QUEUE (DLQ)
A special topic that stores messages which failed processing after all retry attempts are exhausted.

**Example:** A transaction with expired credit card fails 3 times; it moves to DLQ where SAP Commerce admin reviews and contacts customer manually.

---

### 15. SCHEMA REGISTRY
A service that manages and validates schemas to ensure compatibility between producers and consumers.

**Example:** PayPal adds "currencyCode" field to transaction schema; old SAP Commerce still works because Schema Registry maintains backward compatibility.

---

### 16. CUSTOM PARTITIONER
A user-defined strategy that determines which partition a message routes to based on business logic or message content.

**Example:** Custom logic routes PayPal to partition 0, Stripe to partition 1, high-value transactions above 10,000 dollars to partition 2 for special handling.

---

### 17. IDEMPOTENT PRODUCER
A producer configuration that ensures exactly-once delivery without duplicates even when retries occur.

**Example:** Network failure causes PayPal to retry same transaction; idempotent producer ensures customer gets charged only once, not twice.

---

### 18. TRANSACTIONAL PRODUCER
A producer that can atomically send multiple messages across partitions where all messages succeed together or fail together.

**Example:** Payment success sends both "payment-confirmed" and "inventory-reserved" events; if inventory fails to reserve, payment also rolls back automatically.

---

## Producer Configuration

| Property | Explanation |
|----------|-------------|
| `BOOTSTRAP_SERVERS_CONFIG` | Tells producer where to find your Kafka cluster brokers to establish initial connection |
| `ENABLE_IDEMPOTENCE_CONFIG` | Ensures retries never create duplicate messages by assigning sequence numbers to each message |
| `ACKS_CONFIG` | Controls how many broker replicas must confirm receipt before producer considers send successful |
| `RETRIES_CONFIG` | How many times producer resends a message if the broker doesn't acknowledge it |
| `MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION` | How many unacknowledged messages can be in flight simultaneously per broker connection |
| `KEY_SERIALIZER_CLASS_CONFIG` | Converts your message key (like userId string) into bytes so Kafka can transmit and partition it |
| `VALUE_SERIALIZER_CLASS_CONFIG` | Converts your message value (like AvroUser object) into bytes so Kafka can transmit it |
| `SCHEMA_REGISTRY_URL_CONFIG` | Tells producer where to fetch and register Avro schemas for serialization validation |
| `VALUE_SUBJECT_NAME_STRATEGY` | Determines what name Schema Registry uses to store your schema (record name vs topic name) |
| `PARTITIONER_CLASS_CONFIG` | Custom logic that decides which partition number a message routes to based on key or value |
| `TRANSACTIONAL_ID_PREFIX` | Enables exactly-once transactions by giving producer a unique ID that survives restarts |

---

## Factory & Template

| Component | Explanation |
|-----------|-------------|
| **ProducerFactory** | Creates actual KafkaProducer instances using all the config properties you defined |
| **KafkaTemplate** | Wrapper that provides simple send() method and handles threading, callbacks, and transactions automatically |

---

## DeadLetterPublishingRecoverer

| Component | What It Does |
|-----------|---------------|
| `DeadLetterPublishingRecoverer` | After all retries fail, this takes the failed message and publishes it to a dead letter queue topic instead of just dropping it |

### Breaking Down Each Part

| Code | What It Does |
|------|---------------|
| `new DeadLetterPublishingRecoverer(template, (record, ex) -> {...})` | Creates recoverer that uses your KafkaTemplate to send failed messages to DLQ |
| `record.topic() + ".DLT"` | Takes original topic name like "user-topic" and adds ".DLT" to create "user-topic.DLT" |
| `return new TopicPartition(dlqTopic, record.partition())` | Sends failed message to same partition number in DLQ that it came from in original topic |
| `recoverer.setHeadersFunction(...)` | Adds extra metadata as headers so you know why and where the message failed |
| `headers.add("original-topic", ...)` | Stores original topic name so you can reprocess to correct topic later |
| `headers.add("original-partition", ...)` | Stores original partition number for debugging |
| `headers.add("original-offset", ...)` | Stores original offset position for tracking |
| `headers.add("error-message", ...)` | Stores the exception reason like "invalid data" or "connection timeout" |
| `headers.add("error-timestamp", ...)` | Stores when the failure happened for time-based debugging |

---

## DefaultErrorHandler

| Code | What It Does |
|------|---------------|
| `new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3))` | Creates error handler that retries failed messages 3 times, waiting 1 second between each retry |
| `FixedBackOff(1000L, 3)` | 1000L = wait 1 second before retry, 3 = maximum 3 retry attempts |
| `handler.addNotRetryableExceptions(...)` | These exceptions go straight to DLQ without any retry because retrying won't fix them |

---

## ConcurrentKafkaListenerContainerFactory

| Component | What It Does |
|-----------|---------------|
| `ConcurrentKafkaListenerContainerFactory` | Creates containers that run multiple consumer threads in parallel to listen to Kafka partitions |

| Code | What It Does |
|------|---------------|
| `factory.setConsumerFactory(...)` | Tells the factory which consumer configuration to use (bootstrap servers, deserializers, group id) |
| `factory.setCommonErrorHandler(errorHandler)` | Attaches your retry and DLQ logic so failed messages are handled consistently across all listeners |
| `factory.getContainerProperties().setAckMode(MANUAL)` | You must call `ack.acknowledge()` in your code; Kafka won't auto-commit offsets |
| `factory.setConcurrency(3)` | Runs 3 consumer threads in parallel, each handling different partitions |
| `factory.setRecordFilterStrategy(...)` | Filters out records before they reach your `@KafkaListener` method |

---

## Consumer Configuration Properties

| Property | What It Does |
|----------|---------------|
| `BOOTSTRAP_SERVERS_CONFIG` | Tells consumer where to find your Kafka cluster brokers to start consuming messages |
| `AUTO_OFFSET_RESET_CONFIG` | When no committed offset exists, start reading from the beginning (earliest) of the partition |
| `KEY_DESERIALIZER_CLASS_CONFIG` | Converts message key bytes back into original type (String) so consumer can read it |
| `VALUE_DESERIALIZER_CLASS_CONFIG` | Wraps your actual deserializer to handle corrupted messages without crashing the consumer |
| `ENABLE_AUTO_COMMIT_CONFIG` | When false, you must manually call `ack.acknowledge()` to save your position in the partition |
| `PARTITION_ASSIGNMENT_STRATEGY_CONFIG` | Defines how partitions are distributed among consumers in same group when rebalancing happens |

---

### AUTO_OFFSET_RESET Values

| Value | What Happens |
|-------|---------------|
| `"earliest"` | New consumer starts from message 0 in each partition |
| `"latest"` | New consumer starts from newest messages, skips all old messages |
| `"none"` | Consumer throws exception if no committed offset exists |

---

### ErrorHandlingDeserializer

| Code | What It Does |
|------|---------------|
| `ErrorHandlingDeserializer.class` | Wraps your real deserializer (like KafkaAvroDeserializer) and catches deserialization errors |
| Without it | Corrupted message crashes your entire consumer |
| With it | Failed deserialization sends to error handler → retry or DLQ, consumer keeps running |

---

### Partition Assignment Strategies

| Strategy | What It Does |
|----------|---------------|
| `RangeAssignor` | Default: assigns consecutive partitions to each consumer (Consumer1 gets 0,1; Consumer2 gets 2,3) |
| `RoundRobinAssignor` | Alternates partitions like dealing cards (Consumer1 gets 0,2; Consumer2 gets 1,3) |
| `CooperativeStickyAssignor` | Minimizes partition movement during rebalance for zero downtime rolling restarts |

---

## Running the Application

### Prerequisites
- Docker and Docker Compose
- Java 17
- Maven

### Start Infrastructure
```bash
docker-compose up -d
