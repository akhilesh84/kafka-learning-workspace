package com.learnkafka.producer.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class ProducerMetrics {
    private final MeterRegistry registry;
    private final Map<String, Counter> publishedCounters = new ConcurrentHashMap<>();
    private final Map<String, Counter> errorCounters = new ConcurrentHashMap<>();
    private final Timer pollTimer;
    private final Timer publishLatencyTimer;

    public ProducerMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.pollTimer = Timer.builder("kafka_producer_api_poll_duration_seconds")
            .description("Duration of Coinbase API poll requests in seconds")
            .register(registry);
        this.publishLatencyTimer = Timer.builder("kafka_producer_publish_latency_seconds")
            .description("Round-trip latency to Kafka broker for message ACK in seconds")
            .register(registry);
    }

    public void recordEventPublished(String symbol, int partition) {
        String key = symbol + ":" + partition;
        publishedCounters.computeIfAbsent(key, k -> Counter.builder("kafka_producer_events_published_total")
            .tag("symbol", symbol)
            .tag("partition", String.valueOf(partition))
            .description("Total market price events successfully published to Kafka")
            .register(registry)
        ).increment();
    }

    public void recordApiError(String symbol) {
        errorCounters.computeIfAbsent(symbol, s -> Counter.builder("kafka_producer_api_errors_total")
            .tag("symbol", s)
            .description("Total errors polling market price API")
            .register(registry)
        ).increment();
    }

    public Timer getPollTimer() {
        return pollTimer;
    }

    public Timer getPublishLatencyTimer() {
        return publishLatencyTimer;
    }
}
