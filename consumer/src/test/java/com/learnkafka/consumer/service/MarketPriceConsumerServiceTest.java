package com.learnkafka.consumer.service;

import com.learnkafka.common.model.MarketPriceEvent;
import com.learnkafka.consumer.metrics.ConsumerMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.kafka.support.Acknowledgment;

import java.math.BigDecimal;
import java.time.Instant;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class MarketPriceConsumerServiceTest {

    private ConsumerMetrics metrics;
    private PriceAnalyticsService analyticsService;
    private MarketPriceConsumerService consumerService;
    private Acknowledgment acknowledgment;

    @BeforeEach
    void setUp() {
        metrics = new ConsumerMetrics(new SimpleMeterRegistry());
        analyticsService = Mockito.mock(PriceAnalyticsService.class);
        consumerService = new MarketPriceConsumerService(metrics, analyticsService);
        acknowledgment = Mockito.mock(Acknowledgment.class);
    }

    @Test
    void testOnMessageProcessesAndAcknowledges() {
        MarketPriceEvent event = new MarketPriceEvent(
            "evt-999",
            "ETH-USD",
            new BigDecimal("2410.75"),
            "USD",
            Instant.now(),
            "Coinbase Public API"
        );
        ConsumerRecord<String, MarketPriceEvent> record =
            new ConsumerRecord<>("test-topic", 1, 42L, "ETH-USD", event);

        consumerService.onMessage(record, acknowledgment);

        verify(analyticsService, times(1)).processEvent(event, 1, 42L);
        verify(acknowledgment, times(1)).acknowledge();
    }
}
