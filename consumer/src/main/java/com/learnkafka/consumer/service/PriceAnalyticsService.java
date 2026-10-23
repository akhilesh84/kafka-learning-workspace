package com.learnkafka.consumer.service;

import com.learnkafka.common.model.MarketPriceEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class PriceAnalyticsService {
    private static final Logger log = LoggerFactory.getLogger(PriceAnalyticsService.class);

    private final Map<String, BigDecimal> previousPrices = new ConcurrentHashMap<>();
    private final Map<String, BigDecimal> sessionHighs = new ConcurrentHashMap<>();
    private final Map<String, BigDecimal> sessionLows = new ConcurrentHashMap<>();

    public void processEvent(MarketPriceEvent event, int partition, long offset) {
        String symbol = event.symbol();
        BigDecimal price = event.price();

        BigDecimal previous = previousPrices.get(symbol);
        String deltaStr;
        if (previous != null) {
            BigDecimal diff = price.subtract(previous);
            BigDecimal pct = diff.divide(previous, 4, RoundingMode.HALF_UP)
                .multiply(BigDecimal.valueOf(100)).setScale(2, RoundingMode.HALF_UP);
            if (diff.compareTo(BigDecimal.ZERO) >= 0) {
                deltaStr = String.format("+%.2f%% ▲", pct.doubleValue());
            } else {
                deltaStr = String.format("%.2f%% ▼", pct.doubleValue());
            }
        } else {
            deltaStr = "  NEW   ";
        }

        previousPrices.put(symbol, price);
        sessionHighs.compute(symbol, (k, v) -> v == null ? price : v.max(price));
        sessionLows.compute(symbol, (k, v) -> v == null ? price : v.min(price));

        log.info("[CONSUMED] partition={} | offset={} | symbol={} | price=${} | delta={} | sessionMin=${} | sessionMax=${}",
            partition, offset, symbol, price, deltaStr, sessionLows.get(symbol), sessionHighs.get(symbol));
    }
}
