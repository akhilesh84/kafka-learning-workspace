package com.learnkafka.producer.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.common.model.CoinbaseResponse;
import com.learnkafka.common.model.MarketPriceEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Polls the Coinbase public spot price API with resilience fallback.
 */
@Component
public class MarketDataApiClient {
    private static final Logger log = LoggerFactory.getLogger(MarketDataApiClient.class);
    private static final String API_TEMPLATE = "https://api.coinbase.com/v2/prices/%s/spot";

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final Map<String, BigDecimal> lastKnownPrices = new ConcurrentHashMap<>();

    public MarketDataApiClient() {
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .build();
        this.objectMapper = new ObjectMapper();

        // Seed default baseline prices for fallback simulation if internet is offline
        lastKnownPrices.put("BTC-USD", new BigDecimal("81500.00"));
        lastKnownPrices.put("ETH-USD", new BigDecimal("2390.00"));
        lastKnownPrices.put("SOL-USD", new BigDecimal("178.00"));
    }

    public Optional<MarketPriceEvent> fetchSpotPrice(String symbol) {
        String url = String.format(API_TEMPLATE, symbol);
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(4))
                .header("Accept", "application/json")
                .GET()
                .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                CoinbaseResponse coinbaseResponse = objectMapper.readValue(response.body(), CoinbaseResponse.class);
                if (coinbaseResponse != null && coinbaseResponse.data() != null) {
                    BigDecimal price = coinbaseResponse.data().amount();
                    lastKnownPrices.put(symbol, price);
                    return Optional.of(MarketPriceEvent.of(symbol, price, "USD", "Coinbase Public API"));
                }
            } else {
                log.warn("API request failed for {} with status {}", symbol, response.statusCode());
            }
        } catch (Exception e) {
            log.warn("Network error reaching Coinbase API for {}: {}. Using synthetic fallback.", symbol, e.getMessage());
        }

        // Resilient fallback: jitter the last known price by +/- 0.2% so data stream continues
        BigDecimal basePrice = lastKnownPrices.getOrDefault(symbol, new BigDecimal("100.00"));
        double jitterPercent = (ThreadLocalRandom.current().nextDouble() - 0.49) * 0.004;
        BigDecimal syntheticPrice = basePrice.multiply(BigDecimal.valueOf(1.0 + jitterPercent))
            .setScale(2, RoundingMode.HALF_UP);
        lastKnownPrices.put(symbol, syntheticPrice);

        return Optional.of(MarketPriceEvent.of(symbol, syntheticPrice, "USD", "Synthetic Local Fallback"));
    }
}
