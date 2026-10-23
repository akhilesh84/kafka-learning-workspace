package com.learnkafka.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class ProducerApplication implements CommandLineRunner {
    private static final Logger log = LoggerFactory.getLogger(ProducerApplication.class);

    private final Environment env;

    public ProducerApplication(Environment env) {
        this.env = env;
    }

    public static void main(String[] args) {
        SpringApplication.run(ProducerApplication.class, args);
    }

    @Override
    public void run(String... args) {
        log.info("==========================================================");
        log.info(" Starting Market Price Spring Boot Kafka Producer");
        log.info(" Bootstrap Servers : {}", env.getProperty("spring.kafka.bootstrap-servers"));
        log.info(" Target Topic      : {}", env.getProperty("app.topic-name"));
        log.info(" Symbols to Track  : {}", env.getProperty("app.symbols"));
        log.info(" Poll Interval     : {}s", env.getProperty("app.poll-interval-seconds"));
        log.info(" OTLP Metrics URL  : {}", env.getProperty("management.otlp.metrics.export.url"));
        log.info("==========================================================");
    }
}
