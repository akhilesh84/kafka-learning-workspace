package com.learnkafka.common.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Domain record representing a real-time market price event.
 * Published to Kafka with the asset symbol as the partition key.
 */
public record MarketPriceEvent(
    String eventId,
    String symbol,
    BigDecimal price,
    String currency,
    Instant timestamp,
    String source
) {
    public static MarketPriceEvent of(String symbol, BigDecimal price, String currency, String source) {
        return new MarketPriceEvent(
            UUID.randomUUID().toString(),
            symbol,
            price,
            currency,
            Instant.now(),
            source
        );
    }
}
