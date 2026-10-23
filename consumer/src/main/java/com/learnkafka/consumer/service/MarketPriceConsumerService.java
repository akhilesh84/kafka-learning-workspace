package com.learnkafka.consumer.service;

import com.learnkafka.common.model.MarketPriceEvent;
import com.learnkafka.consumer.metrics.ConsumerMetrics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class MarketPriceConsumerService {
    private static final Logger log = LoggerFactory.getLogger(MarketPriceConsumerService.class);

    private final ConsumerMetrics metrics;
    private final PriceAnalyticsService analyticsService;

    public MarketPriceConsumerService(ConsumerMetrics metrics, PriceAnalyticsService analyticsService) {
        this.metrics = metrics;
        this.analyticsService = analyticsService;
    }

    @KafkaListener(
        topics = "${app.topic-name:market-prices}",
        groupId = "${spring.kafka.consumer.group-id:price-analytics-group}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void onMessage(ConsumerRecord<String, MarketPriceEvent> record, Acknowledgment acknowledgment) {
        long startTime = System.nanoTime();
        try {
            MarketPriceEvent event = record.value();
            if (event != null) {
                analyticsService.processEvent(event, record.partition(), record.offset());
                metrics.recordEventProcessed(event.symbol(), record.partition(), event.price());
            }
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }
        } catch (Exception e) {
            log.error("Error processing record partition={} offset={}: {}", record.partition(), record.offset(), e.getMessage(), e);
            throw e;
        } finally {
            metrics.getBatchProcessTimer().record(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
        }
    }
}
