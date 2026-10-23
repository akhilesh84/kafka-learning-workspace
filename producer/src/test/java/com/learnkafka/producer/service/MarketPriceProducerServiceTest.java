package com.learnkafka.producer.service;

import com.learnkafka.common.model.MarketPriceEvent;
import com.learnkafka.producer.metrics.ProducerMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class MarketPriceProducerServiceTest {

    private KafkaTemplate<String, MarketPriceEvent> kafkaTemplate;
    private ProducerMetrics metrics;
    private MarketPriceProducerService producerService;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        kafkaTemplate = Mockito.mock(KafkaTemplate.class);
        metrics = new ProducerMetrics(new SimpleMeterRegistry());
        producerService = new MarketPriceProducerService(kafkaTemplate, metrics, "test-market-prices");
    }

    @Test
    @SuppressWarnings("unchecked")
    void testPublishEventSuccess() {
        MarketPriceEvent event = new MarketPriceEvent(
            "evt-1",
            "BTC-USD",
            new BigDecimal("82000.50"),
            "USD",
            Instant.now(),
            "Coinbase Public API"
        );

        RecordMetadata metadata = new RecordMetadata(
            new TopicPartition("test-market-prices", 0),
            0L, 0, System.currentTimeMillis(), 0, 0
        );
        SendResult<String, MarketPriceEvent> sendResult = new SendResult<>(null, metadata);
        CompletableFuture<SendResult<String, MarketPriceEvent>> future = CompletableFuture.completedFuture(sendResult);

        when(kafkaTemplate.send(eq("test-market-prices"), eq("BTC-USD"), any(MarketPriceEvent.class)))
            .thenReturn(future);

        CompletableFuture<SendResult<String, MarketPriceEvent>> resultFuture = producerService.publishEvent(event);
        assertNotNull(resultFuture);
        verify(kafkaTemplate, times(1)).send("test-market-prices", "BTC-USD", event);
    }
}
