package com.learnkafka.producer.poller;

import com.learnkafka.common.model.MarketPriceEvent;
import com.learnkafka.producer.client.MarketDataApiClient;
import com.learnkafka.producer.metrics.ProducerMetrics;
import com.learnkafka.producer.service.MarketPriceProducerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Component
public class MarketPricePoller {
    private static final Logger log = LoggerFactory.getLogger(MarketPricePoller.class);

    private final MarketDataApiClient apiClient;
    private final MarketPriceProducerService producerService;
    private final ProducerMetrics metrics;
    private final List<String> symbols;

    public MarketPricePoller(MarketDataApiClient apiClient,
                             MarketPriceProducerService producerService,
                             ProducerMetrics metrics,
                             @Value("${app.symbols:BTC-USD,ETH-USD,SOL-USD}") String symbolsStr) {
        this.apiClient = apiClient;
        this.producerService = producerService;
        this.metrics = metrics;
        this.symbols = Arrays.stream(symbolsStr.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .toList();
    }

    @Scheduled(fixedDelayString = "#{${app.poll-interval-seconds:3} * 1000}")
    public void pollAndPublish() {
        for (String symbol : symbols) {
            long pollStart = System.nanoTime();
            Optional<MarketPriceEvent> eventOpt = apiClient.fetchSpotPrice(symbol);
            metrics.getPollTimer().record(System.nanoTime() - pollStart, TimeUnit.NANOSECONDS);

            eventOpt.ifPresent(producerService::publishEvent);
        }
    }
}
