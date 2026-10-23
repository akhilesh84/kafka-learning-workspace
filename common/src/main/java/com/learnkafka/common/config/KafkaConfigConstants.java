package com.learnkafka.common.config;

public final class KafkaConfigConstants {
    private KafkaConfigConstants() {}

    public static final String DEFAULT_TOPIC = "market-prices";
    public static final String DEFAULT_GROUP_ID = "price-analytics-group";
    public static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String DEFAULT_OTEL_ENDPOINT = "http://localhost:4318/v1/metrics";
    public static final String DEFAULT_LOKI_URL = "http://localhost:3100/loki/api/v1/push";
}
