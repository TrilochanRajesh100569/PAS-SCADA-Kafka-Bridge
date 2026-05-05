package com.pinkline.kafkabridge.config;

import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.camel.component.activemq.ActiveMQComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * SecurityConfig
 *
 * Wires TLS for ActiveMQ Artemis.
 *   - Production  : broker-url = ssl://host:61617 + JKS truststore
 *   - Local/test  : broker-url = tcp://localhost:61616, truststore path blank → plain TCP
 *
 * Kafka TLS  — configured entirely via application.properties
 *   (camel.component.kafka.ssl-truststore-* / security-protocol=SASL_SSL)
 *
 * RabbitMQ TLS — configured entirely via application.properties
 *   (spring.rabbitmq.ssl.* / port=5671)
 */
@Configuration
public class SecurityConfig {

    private static final Logger log = LoggerFactory.getLogger(SecurityConfig.class);

    @Value("${camel.component.activemq.broker-url}")
    private String artemisUrl;

    // Use artemis.username/password — NOT camel.component.activemq.username/password.
    // Camel's auto-config reads that namespace and tries to apply credentials via
    // SingleConnectionFactory.createConnection(user, pass) which throws UnsupportedOperationException.
    // We set credentials directly on ActiveMQSslConnectionFactory instead.
    @Value("${artemis.username}")
    private String artemisUser;

    @Value("${artemis.password}")
    private String artemisPass;

    @Value("${tls.truststore.path:}")
    private String truststorePath;

    @Value("${tls.truststore.password:}")
    private String truststorePassword;

    // Bean name MUST be "activemq" — Camel resolves components by scheme name.
    // A bean named "activeMQComponent" (camelCase) is NOT found by the activemq:// resolver.
    @Bean(name = "activemq")
    public ActiveMQComponent activeMQComponent() throws Exception {
        ActiveMQSslConnectionFactory factory = new ActiveMQSslConnectionFactory(artemisUrl);
        factory.setUserName(artemisUser);
        factory.setPassword(artemisPass);
        factory.setWatchTopicAdvisories(false);

        if (truststorePath != null && !truststorePath.isBlank()) {
            // Production: enable TLS certificate verification
            factory.setTrustStore(truststorePath);
            factory.setTrustStorePassword(truststorePassword);
            log.info("Artemis TLS enabled — truststore: {}", truststorePath);
        } else {
            // Local/test: no truststore, plain TCP connection
            log.warn("Artemis TLS truststore not configured — using plain connection (local/test only)");
        }

        ActiveMQComponent component = new ActiveMQComponent();
        component.setConnectionFactory(factory);
        // JmsConfiguration also needs credentials — DefaultJmsMessageListenerContainer
        // calls createConnection(username, password) during refresh cycles, using
        // these values (NOT the factory's stored username/password).
        component.setUsername(artemisUser);
        component.setPassword(artemisPass);
        return component;
    }
}
