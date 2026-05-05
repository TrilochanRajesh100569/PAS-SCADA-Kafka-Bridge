package com.pinkline.kafkabridge;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * PAS-SCADA Kafka Bridge
 *
 * Bridges TMS XML messages from ActiveMQ Artemis to SCADA via Kafka and RabbitMQ.
 * GP product (PASInfoConverter) is not modified.
 */
@SpringBootApplication
public class KafkaBridgeApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaBridgeApplication.class, args);
    }
}
