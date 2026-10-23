package com.learnkafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.Environment;

@SpringBootApplication
public class ConsumerApplication implements CommandLineRunner {
    private static final Logger log = LoggerFactory.getLogger(ConsumerApplication.class);

    private final Environment env;

    public ConsumerApplication(Environment env) {
        this.env = env;
    }

    public static void main(String[] args) {
        SpringApplication.run(ConsumerApplication.class, args);
    }

    @Override
    public void run(String... args) {
        log.info("==========================================================");
        log.info(" Starting Market Price Spring Boot Kafka Consumer");
        log.info(" Bootstrap Servers : {}", env.getProperty("spring.kafka.bootstrap-servers"));
        log.info(" Target Topic      : {}", env.getProperty("app.topic-name"));
        log.info(" Consumer Group ID : {}", env.getProperty("spring.kafka.consumer.group-id"));
        log.info(" OTLP Metrics URL  : {}", env.getProperty("management.otlp.metrics.export.url"));
        log.info("==========================================================");
    }
}
