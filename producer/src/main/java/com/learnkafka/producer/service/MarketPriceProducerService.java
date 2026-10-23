package com.learnkafka.producer.service;

import com.learnkafka.common.model.MarketPriceEvent;
import com.learnkafka.producer.metrics.ProducerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Service
public class MarketPriceProducerService {
    private static final Logger log = LoggerFactory.getLogger(MarketPriceProducerService.class);

    private final KafkaTemplate<String, MarketPriceEvent> kafkaTemplate;
    private final ProducerMetrics metrics;
    private final String topic;

    public MarketPriceProducerService(KafkaTemplate<String, MarketPriceEvent> kafkaTemplate,
                                      ProducerMetrics metrics,
                                      @Value("${app.topic-name:market-prices}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.metrics = metrics;
        this.topic = topic;
    }

    public CompletableFuture<SendResult<String, MarketPriceEvent>> publishEvent(MarketPriceEvent event) {
        long startTime = System.nanoTime();
        // Asset symbol is used as the partition key to guarantee per-symbol ordering
        CompletableFuture<SendResult<String, MarketPriceEvent>> future =
            kafkaTemplate.send(topic, event.symbol(), event);

        future.whenComplete((result, exception) -> {
            long latencyNanos = System.nanoTime() - startTime;
            metrics.getPublishLatencyTimer().record(latencyNanos, TimeUnit.NANOSECONDS);

            if (exception != null) {
                log.error("Failed to publish event for key {}: {}", event.symbol(), exception.getMessage());
                metrics.recordApiError(event.symbol());
            } else {
                int partition = result.getRecordMetadata().partition();
                long offset = result.getRecordMetadata().offset();
                metrics.recordEventPublished(event.symbol(), partition);
                log.info("\u001B[32m[PRODUCED]\u001B[0m symbol={} | price=${} | partition={} | offset={} | latency={}ms",
                    event.symbol(), event.price(), partition, offset,
                    TimeUnit.NANOSECONDS.toMillis(latencyNanos));
            }
        });

        return future;
    }

    public KafkaTemplate<String, MarketPriceEvent> getKafkaTemplate() {
        return kafkaTemplate;
    }
}
