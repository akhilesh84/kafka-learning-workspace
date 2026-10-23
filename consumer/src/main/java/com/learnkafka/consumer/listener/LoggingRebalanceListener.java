package com.learnkafka.consumer.listener;

import com.learnkafka.consumer.metrics.ConsumerMetrics;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.stream.Collectors;

public class LoggingRebalanceListener implements ConsumerRebalanceListener {
    private static final Logger log = LoggerFactory.getLogger(LoggingRebalanceListener.class);
    private final ConsumerMetrics metrics;

    public LoggingRebalanceListener(ConsumerMetrics metrics) {
        this.metrics = metrics;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        String partitionList = partitions.stream()
            .map(p -> p.topic() + "-" + p.partition())
            .collect(Collectors.joining(", "));
        log.warn("\u001B[33m[REBALANCE REVOKED]\u001B[0m Partitions revoked from this instance: [{}]", partitionList);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        metrics.recordRebalance();
        String partitionList = partitions.stream()
            .map(p -> p.topic() + "-" + p.partition())
            .collect(Collectors.joining(", "));
        log.info("\u001B[32m[REBALANCE ASSIGNED]\u001B[0m Partitions assigned to this instance: [{}]", partitionList);
    }
}
