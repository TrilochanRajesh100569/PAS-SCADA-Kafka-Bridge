package com.pinkline.kafkabridge.routes;

import com.pinkline.kafkabridge.api.MessageStore;
import com.pinkline.kafkabridge.config.BridgeConfig;
import com.pinkline.kafkabridge.processor.AlarmStateFanoutProcessor;
import com.pinkline.kafkabridge.processor.DecryptExample;
import com.pinkline.kafkabridge.processor.EncryptProcessor;
import com.pinkline.kafkabridge.processor.JsonToXmlProcessor;
import com.pinkline.kafkabridge.processor.ScadaInboundProcessor;
import com.pinkline.kafkabridge.processor.XmlToJsonProcessor;
import java.nio.charset.StandardCharsets;
import org.apache.camel.LoggingLevel;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.RouteDefinition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * KafkaBridgeRoutes — Fully Config-Driven Camel Router
 *
 * ══════════════════════════════════════════════════════════════════════
 * PIPELINE (application.properties → bridge.pipeline)
 * ══════════════════════════════════════════════════════════════════════
 *
 * Controls the delivery chain from Artemis. Change config, restart — done.
 * No code changes needed.
 *
 *   bridge.pipeline=kafka,rabbitmq,mqtt    full chain (default)
 *   bridge.pipeline=mqtt                   direct Artemis → MQTT
 *   bridge.pipeline=kafka,mqtt             Kafka audit + direct MQTT
 *   bridge.pipeline=kafka,rabbitmq         no MQTT (store in brokers only)
 *   bridge.pipeline=kafka                  Kafka only
 *
 * Pipeline steps are executed IN ORDER as listed. The message flows
 * through each step sequentially in a single Camel route per Artemis topic.
 *
 * ══════════════════════════════════════════════════════════════════════
 * RELAY ROUTES (optional — bridge.forward[n])
 * ══════════════════════════════════════════════════════════════════════
 *
 * Optional separate consumer routes (Kafka → RabbitMQ).
 * Only needed if Kafka must be a true relay point (not just a write-through).
 * Leave empty if pipeline write-through is sufficient.
 *
 * ══════════════════════════════════════════════════════════════════════
 * INBOUND ROUTES (optional — bridge.inbound[n])
 * ══════════════════════════════════════════════════════════════════════
 *
 * RabbitMQ → Artemis reverse routes.
 * Used for commands/responses coming back from SCADA to TMS.
 *
 * ══════════════════════════════════════════════════════════════════════
 * MONITOR (optional — bridge.monitor.enabled=true)
 * ══════════════════════════════════════════════════════════════════════
 *
 * Feeds the in-bridge REST API (/api/messages).
 * Only useful if you want a local message viewer inside the bridge app.
 * For production, use the separate SCADA-API microservice instead.
 */
@Component
public class KafkaBridgeRoutes extends RouteBuilder {

    private static final String BYTE_SER   = "org.apache.kafka.common.serialization.ByteArraySerializer";
    private static final String BYTE_DESER = "org.apache.kafka.common.serialization.ByteArrayDeserializer";

    @Autowired
    private BridgeConfig config;

    @Autowired
    private MessageStore messageStore;

    @Autowired
    private ProducerTemplate producerTemplate;

    @Override
    public void configure() {

        // ── Global error handler ──────────────────────────────────────────
        onException(Exception.class)
            .maximumRedeliveries(3)
            .redeliveryDelay(2000)
            .retryAttemptedLogLevel(LoggingLevel.WARN)
            .useOriginalMessage()
            .to("activemq:queue:DLQ.kafka-bridge")
            .log("Dead-letter queue — ${exception.message}")
            .handled(true);

        // ══════════════════════════════════════════════════════════════════
        // OUTBOUND: Artemis → pipeline sinks (one route per Artemis topic)
        //
        // Skipped when bridge.input-kafka.enabled=true — in that mode a
        // Kafka Connect source connector populates the input topic and a
        // single Kafka consumer route below replaces this loop.
        //
        // The pipeline is built dynamically from bridge.pipeline config.
        // Each step in the pipeline adds a .to() call to the route.
        // Order in config = order of delivery.
        // ══════════════════════════════════════════════════════════════════
        if (config.getInputKafka().isEnabled()) {
            log.info("bridge.input-kafka.enabled=true — Artemis-direct routes SKIPPED, "
                   + "consuming from Kafka topic [{}] instead",
                   config.getInputKafka().getTopic());
        } else {
        for (String topic : config.getArtemisTopics().split(",")) {
            topic = topic.trim();
            String routeId = "outbound-" + topic.replace(".", "-").toLowerCase();

            RouteDefinition route = from("activemq:topic:" + topic)
                .routeId(routeId)
                .log("← Artemis [" + topic + "] — pipeline: " + config.getPipeline()
                        + " | encrypt: " + config.getEncrypt().isEnabled())
                .process(new XmlToJsonProcessor());

            if (config.getEncrypt().isEnabled()) {
                route.process(new EncryptProcessor());
            } else {
                // No encryption — convert String → UTF-8 bytes so all sinks receive byte[]
                route.process(e -> e.getIn().setBody(
                        e.getIn().getBody(String.class).getBytes(StandardCharsets.UTF_8)));
                route.log("Encryption disabled — plain JSON forwarded");
            }

            // Build the pipeline dynamically from config
            for (String step : config.getPipeline().split(",")) {
                switch (step.trim().toLowerCase()) {

                    case "kafka" -> {
                        route.to("kafka:" + config.getKafka().getTopic()
                                + "?brokers={{kafka.brokers}}"
                                + "&valueSerializer=" + BYTE_SER
                                + "&keySerializer=" + BYTE_SER);
                        route.log("→ Kafka [" + config.getKafka().getTopic() + "]");
                    }

                    case "rabbitmq" -> {
                        BridgeConfig.RabbitmqOut rmq = config.getRabbitmqOut();
                        route.to("spring-rabbitmq:" + rmq.getExchange()
                                + "?routingKey=" + rmq.getRoutingKey());
                        route.log("→ RabbitMQ [" + rmq.getExchange()
                                + " / " + rmq.getRoutingKey() + "]");
                    }

                    case "mqtt" -> {
                        BridgeConfig.MqttSink mqtt = config.getMqtt();
                        StringBuilder uri = new StringBuilder("paho:")
                                .append(mqtt.getTopic())
                                .append("?brokerUrl=").append(mqtt.getBrokerUrl())
                                .append("&qos=").append(mqtt.getQos())
                                .append("&clientId=").append(mqtt.getClientId())
                                .append("-").append(topic.replace(".", "-").toLowerCase())
                                .append("&automaticReconnect=true")
                                .append("&cleanSession=true")
                                .append("&connectionTimeout=10")
                                .append("&keepAliveInterval=30");
                        if (mqtt.getUsername() != null && !mqtt.getUsername().isBlank()) {
                            uri.append("&userName=").append(mqtt.getUsername())
                               .append("&password=").append(mqtt.getPassword());
                        }
                        route.to(uri.toString());
                        route.log("→ MQTT [" + mqtt.getTopic() + "]");
                    }

                    default -> log.warn("Unknown pipeline step '{}' — skipped", step.trim());
                }
            }
        }
        } // end else (Artemis-direct mode)

        // ══════════════════════════════════════════════════════════════════
        // CONNECT-MODE INPUT: Kafka source → pipeline sinks
        //
        // Activated by bridge.input-kafka.enabled=true. Replaces the
        // Artemis-direct loop above so a Kafka Connect source connector
        // can sit between the TMS Artemis broker and this app.
        //
        // The XmlToJson + Encrypt processors are reused unchanged, so
        // the wire format on the output side stays identical.
        // ══════════════════════════════════════════════════════════════════
        if (config.getInputKafka().isEnabled()) {
            BridgeConfig.InputKafka in = config.getInputKafka();
            RouteDefinition route = from("kafka:" + in.getTopic()
                    + "?brokers={{kafka.brokers}}"
                    + "&groupId=" + in.getConsumerGroup()
                    + "&autoOffsetReset=earliest"
                    + "&valueDeserializer=" + BYTE_DESER
                    + "&keyDeserializer=" + BYTE_DESER)
                .routeId("inbound-kafka-" + in.getTopic().replace(".", "-"))
                .log("← Kafka source [" + in.getTopic() + "] — pipeline: "
                        + config.getPipeline()
                        + " | encrypt: " + config.getEncrypt().isEnabled())
                // Kafka delivers byte[] — convert to String for XmlToJsonProcessor
                .process(e -> e.getIn().setBody(
                        new String(e.getIn().getBody(byte[].class), StandardCharsets.UTF_8)))
                .process(new XmlToJsonProcessor());

            if (config.getEncrypt().isEnabled()) {
                route.process(new EncryptProcessor());
            } else {
                route.process(e -> e.getIn().setBody(
                        e.getIn().getBody(String.class).getBytes(StandardCharsets.UTF_8)));
                route.log("Encryption disabled — plain JSON forwarded");
            }

            // Reuse the same pipeline-step builder as the Artemis branch.
            for (String step : config.getPipeline().split(",")) {
                switch (step.trim().toLowerCase()) {
                    case "kafka" -> {
                        route.to("kafka:" + config.getKafka().getTopic()
                                + "?brokers={{kafka.brokers}}"
                                + "&valueSerializer=" + BYTE_SER
                                + "&keySerializer=" + BYTE_SER);
                        route.log("→ Kafka [" + config.getKafka().getTopic() + "]");
                    }
                    case "rabbitmq" -> {
                        BridgeConfig.RabbitmqOut rmq = config.getRabbitmqOut();
                        route.to("spring-rabbitmq:" + rmq.getExchange()
                                + "?routingKey=" + rmq.getRoutingKey());
                        route.log("→ RabbitMQ [" + rmq.getExchange()
                                + " / " + rmq.getRoutingKey() + "]");
                    }
                    case "mqtt" -> {
                        BridgeConfig.MqttSink mqtt = config.getMqtt();
                        StringBuilder uri = new StringBuilder("paho:")
                                .append(mqtt.getTopic())
                                .append("?brokerUrl=").append(mqtt.getBrokerUrl())
                                .append("&qos=").append(mqtt.getQos())
                                .append("&clientId=").append(mqtt.getClientId())
                                .append("-kafka-source")
                                .append("&automaticReconnect=true")
                                .append("&cleanSession=true")
                                .append("&connectionTimeout=10")
                                .append("&keepAliveInterval=30");
                        if (mqtt.getUsername() != null && !mqtt.getUsername().isBlank()) {
                            uri.append("&userName=").append(mqtt.getUsername())
                               .append("&password=").append(mqtt.getPassword());
                        }
                        route.to(uri.toString());
                        route.log("→ MQTT [" + mqtt.getTopic() + "]");
                    }
                    default -> log.warn("Unknown pipeline step '{}' — skipped", step.trim());
                }
            }
        }

        // ══════════════════════════════════════════════════════════════════
        // REVERSE KAFKA: Kafka encrypted-input → decrypt → optional XML → Kafka
        //
        // Activated by bridge.reverse-kafka.enabled=true. SCADA-side
        // encrypted JSON arrives via a Connect RabbitMQ-source connector
        // on inputTopic; this route decrypts, optionally converts to XML,
        // and writes outputTopic where a Connect Artemis-sink connector
        // drains it back to the TMS Artemis broker.
        // ══════════════════════════════════════════════════════════════════
        if (config.getReverseKafka().isEnabled()) {
            BridgeConfig.ReverseKafka rev = config.getReverseKafka();
            RouteDefinition route = from("kafka:" + rev.getInputTopic()
                    + "?brokers={{kafka.brokers}}"
                    + "&groupId=" + rev.getConsumerGroup()
                    + "&autoOffsetReset=earliest"
                    + "&valueDeserializer=" + BYTE_DESER
                    + "&keyDeserializer=" + BYTE_DESER)
                .routeId("reverse-kafka-" + rev.getInputTopic().replace(".", "-"))
                .log("← Kafka reverse [" + rev.getInputTopic() + "] — decrypt="
                        + config.getEncrypt().isEnabled() + " convertXml=" + rev.isConvertToXml());

            // Decrypt encrypted bytes -> JSON string. ScadaInboundProcessor
            // logs the RSAE type for traceability.
            // bridge.reverse-kafka.encrypt-enabled controls this independently
            // of the forward-path encrypt flag, so SCADA can publish plain JSON
            // while TMS→SCADA stays encrypted.
            if (rev.isEncryptEnabled()) {
                route.process(e -> {
                    byte[] payload = e.getIn().getBody(byte[].class);
                    e.getIn().setBody(DecryptExample.decrypt(payload));
                });
            } else {
                route.process(e -> e.getIn().setBody(
                        new String(e.getIn().getBody(byte[].class), StandardCharsets.UTF_8)));
            }
            route.process(new ScadaInboundProcessor());

            // Fan out alarm state to compacted Kafka topic (flood-control).
            // Skipped when bridge.alarm-state.enabled=false. Pass-through.
            if (config.getAlarmState().isEnabled()) {
                route.process(new AlarmStateFanoutProcessor(
                        producerTemplate, config.getAlarmState().getTopic()));
            }

            if (rev.isConvertToXml()) {
                route.process(new JsonToXmlProcessor());
            }
            // Always emit byte[] to Kafka regardless of upstream String/XML.
            route.process(e -> {
                Object b = e.getIn().getBody();
                if (b instanceof String s) {
                    e.getIn().setBody(s.getBytes(StandardCharsets.UTF_8));
                }
            });
            route.to("kafka:" + rev.getOutputTopic()
                    + "?brokers={{kafka.brokers}}"
                    + "&valueSerializer=" + BYTE_SER
                    + "&keySerializer=" + BYTE_SER);
            route.log("→ Kafka reverse [" + rev.getOutputTopic() + "]");
        }

        // ══════════════════════════════════════════════════════════════════
        // RELAY ROUTES: Kafka → RabbitMQ (optional, one per forward entry)
        //
        // Only needed when Kafka must act as a true relay point.
        // With pipeline write-through, these are not required.
        // Configure bridge.forward[n] to enable.
        // ══════════════════════════════════════════════════════════════════
        for (BridgeConfig.ForwardRoute fwd : config.getForward()) {
            String routeId = "relay-" + fwd.getFromKafka().replace(".", "-")
                           + "-to-" + fwd.getRoutingKey().replace(".", "-");

            from("kafka:" + fwd.getFromKafka()
                    + "?brokers={{kafka.brokers}}"
                    + "&groupId=" + fwd.getConsumerGroup()
                    + "&autoOffsetReset=latest"
                    + "&valueDeserializer=" + BYTE_DESER
                    + "&keyDeserializer=" + BYTE_DESER)
                .routeId(routeId)
                .log("← Kafka relay [" + fwd.getFromKafka() + "]")
                .to("spring-rabbitmq:" + fwd.getToExchange()
                        + "?routingKey=" + fwd.getRoutingKey())
                .log("→ RabbitMQ relay [" + fwd.getToExchange()
                        + " / " + fwd.getRoutingKey() + "]");
        }

        // ══════════════════════════════════════════════════════════════════
        // INBOUND ROUTES: RabbitMQ/MQTT → Artemis (SCADA → TMS direction)
        //
        // SCADA API publishes RSAE JSON to MQTT topic "scada/tms/alarms".
        // RabbitMQ MQTT plugin maps that to routing key "scada.tms.alarms"
        // on amq.topic, which lands in the declared queue.
        // Camel picks up from the queue → optional JSON→XML → Artemis topic.
        //
        // Configure bridge.inbound[n] to enable.
        //
        // Skipped when bridge.reverse-kafka.enabled=true — that path covers
        // the same SCADA→TMS direction via Connect+Kafka, and running both
        // would split messages between two competing consumers on the same
        // RabbitMQ queue.
        // ══════════════════════════════════════════════════════════════════
        if (config.getReverseKafka().isEnabled() && !config.getInbound().isEmpty()) {
            log.info("bridge.reverse-kafka.enabled=true — legacy inbound RabbitMQ→Artemis routes SKIPPED");
        }
        for (BridgeConfig.InboundRoute inb : config.getReverseKafka().isEnabled()
                                              ? java.util.Collections.<BridgeConfig.InboundRoute>emptyList()
                                              : config.getInbound()) {
            String routeId = "inbound-" + inb.getRoutingKey().replace(".", "-")
                           + "-to-" + inb.getToTopic().replace(".", "-");

            String fromUri = "spring-rabbitmq:" + inb.getFromExchange()
                    + "?routingKey=" + inb.getRoutingKey()
                    + "&autoStartup=true";
            if (inb.getQueue() != null && !inb.getQueue().isBlank()) {
                fromUri += "&queues=" + inb.getQueue();
            }

            RouteDefinition route = from(fromUri)
                .routeId(routeId)
                .log("← SCADA inbound [" + inb.getRoutingKey() + "] → Artemis [" + inb.getToTopic() + "]"
                        + (inb.isConvertToXml() ? " (JSON→XML)" : " (JSON pass-through)"))
                .process(new ScadaInboundProcessor());

            if (inb.isConvertToXml()) {
                route.process(new JsonToXmlProcessor());
            }

            route.to("activemq:topic:" + inb.getToTopic())
                 .log("→ Artemis [" + inb.getToTopic() + "] delivered");
        }

        // ══════════════════════════════════════════════════════════════════
        // MONITOR: RabbitMQ → in-bridge API (optional)
        //
        // Feeds the local /api/messages REST endpoint inside this app.
        // Enable with bridge.monitor.enabled=true
        // For production use, prefer the separate SCADA-API microservice.
        // ══════════════════════════════════════════════════════════════════
        if (config.getMonitor().isEnabled()) {
            BridgeConfig.Monitor mon = config.getMonitor();
            from("spring-rabbitmq:" + mon.getFromExchange()
                    + "?queues=" + mon.getQueue()
                    + "&routingKey=" + mon.getRoutingKey()
                    + "&autoStartup=true")
                .routeId("monitor-rabbitmq-to-api")
                .log("← Monitor [" + mon.getRoutingKey() + "]")
                .process(exchange -> {
                    byte[] payload = exchange.getIn().getBody(byte[].class);
                    if (payload == null) {
                        payload = exchange.getIn().getBody(String.class).getBytes();
                    }
                    String json = DecryptExample.decrypt(payload);
                    exchange.getIn().setBody(json);
                })
                .process(exchange -> {
                    String json  = exchange.getIn().getBody(String.class);
                    String topic = exchange.getIn().getHeader("kafka.TOPIC", mon.getRoutingKey(), String.class);
                    messageStore.add(topic, json);
                })
                .log("→ Monitor stored — available at /api/messages");
        }
    }
}
