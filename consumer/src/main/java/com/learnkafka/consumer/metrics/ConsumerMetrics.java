package com.learnkafka.consumer.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class ConsumerMetrics {
    private final MeterRegistry registry;
    private final Map<String, Counter> processedCounters = new ConcurrentHashMap<>();
    private final Map<String, AtomicReference<BigDecimal>> priceGauges = new ConcurrentHashMap<>();
    private final Counter rebalanceCounter;
    private final Timer batchProcessTimer;

    public ConsumerMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.rebalanceCounter = Counter.builder("kafka_consumer_rebalances_total")
            .description("Total consumer group partition rebalance events witnessed")
            .register(registry);
        this.batchProcessTimer = Timer.builder("kafka_consumer_batch_process_duration_seconds")
            .description("Time taken to process a polled batch of records in seconds")
            .register(registry);
    }

    public void recordEventProcessed(String symbol, int partition, BigDecimal price) {
        String key = symbol + ":" + partition;
        processedCounters.computeIfAbsent(key, k -> Counter.builder("kafka_consumer_events_processed_total")
            .tag("symbol", symbol)
            .tag("partition", String.valueOf(partition))
            .description("Total market price events successfully consumed and processed")
            .register(registry)
        ).increment();

        priceGauges.computeIfAbsent(symbol, s -> {
            AtomicReference<BigDecimal> ref = new AtomicReference<>(price);
            registry.gauge("kafka_consumer_price_latest",
                io.micrometer.core.instrument.Tags.of("symbol", s),
                ref,
                r -> r.get() != null ? r.get().doubleValue() : 0.0);
            return ref;
        }).set(price);
    }

    public void recordRebalance() {
        rebalanceCounter.increment();
    }

    public Timer getBatchProcessTimer() {
        return batchProcessTimer;
    }
}
