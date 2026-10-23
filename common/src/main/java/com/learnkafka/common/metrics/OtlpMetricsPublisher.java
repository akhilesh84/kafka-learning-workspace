package com.learnkafka.common.metrics;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.binder.jvm.*;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import io.micrometer.registry.otlp.OtlpConfig;
import io.micrometer.registry.otlp.OtlpMeterRegistry;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;

public class OtlpMetricsPublisher implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(OtlpMetricsPublisher.class);

    private final OtlpMeterRegistry registry;
    private final String serviceName;

    public OtlpMetricsPublisher(String serviceName, String otelUrl) {
        this.serviceName = serviceName;
        log.info("Initializing OTLP Metrics Exporter pushing to {}", otelUrl);

        OtlpConfig config = new OtlpConfig() {
            @Override
            public String get(String key) {
                return null;
            }

            @Override
            public String url() {
                return otelUrl;
            }

            @Override
            public Duration step() {
                return Duration.ofSeconds(5);
            }

            @Override
            public Map<String, String> resourceAttributes() {
                return Map.of("service.name", serviceName);
            }
        };

        this.registry = new OtlpMeterRegistry(config, Clock.SYSTEM);

        // Bind standard JVM telemetry
        new JvmMemoryMetrics().bindTo(registry);
        new JvmGcMetrics().bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);
    }

    public OtlpMeterRegistry getRegistry() {
        return registry;
    }

    public void bindProducer(KafkaProducer<?, ?> producer) {
        new KafkaClientMetrics(producer).bindTo(registry);
    }

    public void bindConsumer(KafkaConsumer<?, ?> consumer) {
        new KafkaClientMetrics(consumer).bindTo(registry);
    }

    @Override
    public void close() {
        log.info("Closing OtlpMeterRegistry and flushing metrics...");
        registry.close();
    }
}
