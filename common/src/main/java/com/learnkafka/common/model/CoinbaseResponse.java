package com.learnkafka.common.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.math.BigDecimal;

/**
 * DTO matching Coinbase Spot Price API response:
 * {"data":{"base":"BTC","currency":"USD","amount":"81546.89"}}
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CoinbaseResponse(Data data) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Data(
        String base,
        String currency,
        BigDecimal amount
    ) {}
}
