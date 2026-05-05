package com.pinkline.kafkabridge.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * ScadaInboundProcessor
 *
 * Handles RSAE messages arriving from the SCADA API via RabbitMQ/MQTT.
 * Converts the payload to a UTF-8 String and logs the RSAE type so
 * operators can trace the reverse flow in logs.
 *
 * RSAE types published by SCADA API:
 *   UpdateAlarm   — equipment state change
 *   KeepAlive     — heartbeat
 *   SendAllAlarms — full alarm broadcast
 *   GetAllAlarms  — request TMS alarm state
 */
public class ScadaInboundProcessor implements Processor {

    private static final Logger log = LoggerFactory.getLogger(ScadaInboundProcessor.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void process(Exchange exchange) throws Exception {
        Object raw = exchange.getIn().getBody();

        String json;
        if (raw instanceof byte[] bytes) {
            json = new String(bytes, StandardCharsets.UTF_8);
        } else {
            json = exchange.getIn().getBody(String.class);
        }

        if (json == null || json.isBlank()) {
            log.warn("ScadaInbound — empty payload, skipping");
            exchange.setRouteStop(true);
            return;
        }

        // Log the RSAE type for traceability
        try {
            JsonNode root = mapper.readTree(json);
            String type = root.path("type").asText("unknown");
            String creatorId = root.path("creatorId").asText("-");
            log.info("ScadaInbound ← RSAE type={} creatorId={} len={}",
                    type, creatorId, json.length());
        } catch (Exception e) {
            log.warn("ScadaInbound — could not parse JSON type field: {}", e.getMessage());
        }

        // Pass as String to Artemis (TMS consumes as TextMessage)
        exchange.getIn().setBody(json);
    }
}
