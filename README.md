# Kafka Concepts Learning Workspace (Java 21, Gradle & Local Kubernetes)

A comprehensive hands-on learning workspace designed to master Apache Kafka concepts using Java 21 console applications, push-based OpenTelemetry telemetry, Grafana Loki log sinking, and a full standalone Kubernetes observability stack.

---

## Architecture Overview

```
                                      +--------------------------+
                                      | Coinbase Public REST API |
                                      | (/v2/prices/{asset}/spot)|
                                      +-------------+------------+
                                                    | (Polls BTC, ETH, SOL)
                                                    v
+------------------------+             +------------+-------------+
| Grafana Web Dashboard  |             |  Producer Console App    |
| (Port 3000)            |             |  (Key = Asset Symbol)    |
+-----------+------------+             +------------+-------------+
            |                                       |
            | Queries Logs & Metrics                | (Publishes Keyed Events)
            v                                       v
+-----------+------------+             +------------+-------------+
| Loki (3100) /          |             | Apache Kafka 3.8 (KRaft) |
| Prometheus (9090) /    |             | Topic: market-prices     |
| OTel Collector (4318)  |             | (3 Partitions: 0, 1, 2)  |
+------------------------+             +------------+-------------+
            ^                                       |
            | Pushes OTLP Metrics & Logs            | (Consumes by Partition)
            |                                       v
+-----------+------------+             +------------+-------------+
|  Consumer Console App  |<------------+ Consumer Console App     |
|  (Replica 2 / P: 1)    |             | (Replica 1 / P: 0, 2)    |
+------------------------+             +--------------------------+
```

### Key Highlights
- **Spring for Apache Kafka**: Producer uses Spring's `KafkaTemplate` with asynchronous `CompletableFuture` callbacks; consumer uses `@KafkaListener` with `MANUAL_IMMEDIATE` container acknowledgment.
- **Spring Boot 3 & Actuator**: Managed container lifecycle, externalized `application.yml` configuration with environment variable overrides, and Micrometer OTLP metrics.
- **Real-Time Online Data**: Producer continuously polls the live Coinbase Spot Price API for `BTC-USD`, `ETH-USD`, and `SOL-USD`.
- **Partition Key Ordering**: Events are keyed by asset symbol (`BTC-USD`, `ETH-USD`, `SOL-USD`), guaranteeing that all events for a given asset route to the exact same partition in strict chronological order.
- **Enterprise Delivery Semantics**: Producer runs with `enable.idempotence=true` and `acks=all`. Consumer uses manual offset commits (`Acknowledgment.acknowledge()`) for at-least-once processing.
- **Push-Based OTLP Telemetry**: Applications push metrics via HTTP to the **OpenTelemetry Collector** (`:4318`), which Prometheus scrapes (`:8889`), and Grafana queries (`:3000`).
- **Centralized Log Sink**: Both applications ship structured logs directly to **Grafana Loki** (`:3100`) via Logback's `Loki4jAppender` while maintaining colored stdout console logs.
- **Standalone Observability**: Kafka, Kafka UI, Loki, OTel Collector, Prometheus, and Grafana are 100% decoupled standalone Kubernetes deployments.

---

## Project Structure

```
~/MyDevArea/kafka-learning-workspace/
├── build.gradle                  # Root Gradle build (Java 21, Spring Boot 3.3.4 plugins)
├── settings.gradle               # Multi-project definition (common, producer, consumer)
├── gradlew                       # Gradle wrapper script
├── common/                       # Shared domain models, SerDes, OTLP publisher
│   └── src/main/java/com/learnkafka/common/
│       ├── model/MarketPriceEvent.java
│       ├── model/CoinbaseResponse.java
│       ├── serde/JsonSerializer.java
│       ├── serde/JsonDeserializer.java
│       └── metrics/OtlpMetricsPublisher.java
├── producer/                     # Spring Boot Producer application
│   ├── Dockerfile
│   └── src/main/
│       ├── resources/application.yml
│       └── java/com/learnkafka/producer/
│           ├── ProducerApplication.java
│           ├── config/KafkaProducerConfig.java
│           ├── service/MarketPriceProducerService.java
│           ├── poller/MarketPricePoller.java
│           ├── client/MarketDataApiClient.java
│           └── metrics/ProducerMetrics.java
├── consumer/                     # Spring Boot Consumer application
│   ├── Dockerfile
│   └── src/main/
│       ├── resources/application.yml
│       └── java/com/learnkafka/consumer/
│           ├── ConsumerApplication.java
│           ├── config/KafkaConsumerConfig.java
│           ├── listener/LoggingRebalanceListener.java
│           ├── service/MarketPriceConsumerService.java
│           ├── service/PriceAnalyticsService.java
│           └── metrics/ConsumerMetrics.java
├── k8s/                          # Kubernetes manifests
│   ├── 00-namespace.yaml
│   ├── 01-kafka-kraft.yaml       # Single-pod Kafka KRaft broker
│   ├── 02-kafka-ui.yaml          # Web dashboard (port 8080)
│   ├── 03-loki.yaml              # Log sink (port 3100)
│   ├── 04-otel-collector.yaml    # Ingests OTLP (:4318), exports Prometheus (:8889)
│   ├── 05-prometheus.yaml        # Time-series metrics engine (port 9090)
│   ├── 06-grafana.yaml           # Dashboards (port 3000, pre-mounted Loki & Prom)
│   ├── 07-configmap.yaml         # Shared environment variables
│   ├── 08-consumer-deployment.yaml # Consumer pod (scalable 1..3)
│   └── 09-producer-deployment.yaml # Producer pod
└── scripts/
    ├── build-all.sh              # Compiles jars and builds Docker images
    ├── deploy-k8s.sh             # Deploys stack to Kubernetes
    ├── teardown-k8s.sh           # Cleanly deletes Kubernetes namespace
    ├── run-producer-local.sh     # Runs producer on host terminal
    └── run-consumer-local.sh     # Runs consumer on host terminal
```

---

## Quickstart: Deploy to Local Kubernetes

### 1. Build Artifacts & Docker Images
From the project root directory:
```bash
./scripts/build-all.sh
```

### 2. Deploy to Kubernetes
```bash
./scripts/deploy-k8s.sh
```
This deploys the namespace, Kafka KRaft, Kafka UI, Loki, OTel Collector, Prometheus, Grafana, and the Producer/Consumer pods.

Verify all pods are running:
```bash
kubectl get pods -n kafka-learning
```

---

## Interactive Learning Labs

### Lab 1: Inspect Live Data in Kafka UI
Port-forward the Kafka UI service:
```bash
kubectl port-forward -n kafka-learning svc/kafka-ui 8080:8080
```
Open your browser at **`http://localhost:8080`**:
- Navigate to **Topics** -> **`market-prices`**.
- Inspect the **Messages** tab. Notice how `BTC-USD` always goes to the same partition, `ETH-USD` to another, and `SOL-USD` to another!

### Lab 2: Tail Producer & Consumer Live Console Streams
In one terminal, view the producer polling the online API:
```bash
kubectl logs -n kafka-learning -l app=kafka-producer -f
```
Output:
```
[PRODUCED] symbol=BTC-USD | price=$81546.89 | partition=1 | offset=42 | latency=18ms
[PRODUCED] symbol=ETH-USD | price=$2394.62  | partition=0 | offset=38 | latency=12ms
[PRODUCED] symbol=SOL-USD | price=$178.40   | partition=2 | offset=31 | latency=14ms
```

In a second terminal, view the consumer processing events with running delta analytics:
```bash
kubectl logs -n kafka-learning -l app=kafka-consumer -f
```
Output:
```
[CONSUMED] p:1 off:42   | BTC-USD : $81546.89    | delta: +0.14% ▲ | [min: $81420.00, max: $81610.00]
[CONSUMED] p:0 off:38   | ETH-USD : $2394.62     | delta: -0.05% ▼ | [min: $2385.00,  max: $2402.00]
[CONSUMED] p:2 off:31   | SOL-USD : $178.40      | delta: +0.82% ▲ | [min: $176.50,   max: $179.20]
```

### Lab 3: Witness Live Consumer Group Rebalancing
Currently, 1 consumer pod handles all 3 partitions (0, 1, and 2). Scale the consumer deployment to 3 replicas:
```bash
kubectl scale deployment kafka-consumer -n kafka-learning --replicas=3
```
Watch the rebalance events in the logs:
```bash
kubectl logs -n kafka-learning -l app=kafka-consumer --tail=30 -f
```
You will observe `LoggingRebalanceListener` output:
```
[REBALANCE REVOKED] Partitions revoked from this instance: [market-prices-0, market-prices-1, market-prices-2]
[REBALANCE ASSIGNED] Partitions assigned to this instance: [market-prices-1]
```
Each consumer pod now handles exactly 1 partition! Scale back to 1 replica (`kubectl scale deployment kafka-consumer -n kafka-learning --replicas=1`) and observe partition reassignment back to the single worker.

### Lab 4: Explore Logs & Metrics in Grafana
Port-forward Grafana:
```bash
kubectl port-forward -n kafka-learning svc/grafana 3000:3000
```
Open **`http://localhost:3000`** (anonymous login enabled):
- Click **Explore** in the left menu.
- **Logs via Loki**:
  - Select the **Loki** data source.
  - Run query: `{app="kafka-producer"}` or `{app="kafka-consumer"}` to view live streaming log lines.
- **Metrics via Prometheus**:
  - Select the **Prometheus** data source.
  - Run query: `kafka_producer_events_published_total` or `kafka_consumer_price_latest{symbol="BTC-USD"}` to graph published events and prices in real time.

---

## Teardown
To cleanly remove all Kubernetes pods, deployments, and services:
```bash
./scripts/teardown-k8s.sh
```
