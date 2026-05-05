package com.pinkline.kafkabridge.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * AlarmStateFanoutProcessor
 *
 * For each RSAE message that carries alarm state (UpdateAlarm or
 * SendAllAlarms), produces one record per alarm to a compacted Kafka
 * topic keyed by alarmId. The compacted topic retains only the latest
 * record per alarmId, so a fresh consumer or a snapshot replay sees
 * current truth instead of replaying the full event history.
 *
 * KeepAlive and GetAllAlarms carry no state — passed through unchanged.
 *
 * Input  (Exchange body): JSON String (decrypted RSAE envelope)
 * Output (Exchange body): unchanged — pipeline continues to scada.tms.processed
 *
 * The value written to the compacted topic is the AES-GCM encrypted
 * UpdateAlarm envelope (base64-encoded), so the snapshot replay path
 * can publish to RabbitMQ unchanged and SCADA decrypts as normal.
 */
public class AlarmStateFanoutProcessor implements Processor {

    private static final Logger log = LoggerFactory.getLogger(AlarmStateFanoutProcessor.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private final ProducerTemplate producer;
    private final String topic;

    public AlarmStateFanoutProcessor(ProducerTemplate producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        String json = exchange.getIn().getBody(String.class);
        if (json == null || json.isBlank()) return;

        JsonNode root = mapper.readTree(json);
        // Payloads use either lowercase or PascalCase depending on producer.
        String type = textOr(root, "Type", textOr(root, "type", ""));
        String creatorId = textOr(root, "CreatorId", textOr(root, "creatorId", ""));

        int produced = 0;
        if ("UpdateAlarm".equalsIgnoreCase(type)) {
            JsonNode alarm = firstPresent(root, "Alarm", "alarm");
            if (sendOne(alarm, creatorId)) produced++;
        } else if ("SendAllAlarms".equalsIgnoreCase(type)) {
            JsonNode alarms = firstPresent(root, "Alarms", "alarms");
            if (alarms != null && alarms.isArray()) {
                for (JsonNode a : alarms) {
                    if (sendOne(a, creatorId)) produced++;
                }
            }
        } else {
            // KeepAlive / GetAllAlarms / unknown: nothing to compact.
            return;
        }

        if (produced > 0) {
            log.info("AlarmStateFanout — type={} produced={} record(s) to {}",
                    type, produced, topic);
        }
    }

    private boolean sendOne(JsonNode alarm, String creatorId) throws Exception {
        if (alarm == null || alarm.isMissingNode() || alarm.isNull()) return false;
        String id = textOr(alarm, "Id", textOr(alarm, "id", ""));
        if (id.isBlank()) {
            log.warn("AlarmStateFanout — alarm record missing Id, skipping: {}", alarm);
            return false;
        }
        String state = textOr(alarm, "State", textOr(alarm, "state", ""));
        String ts    = textOr(alarm, "Timestamp", textOr(alarm, "timestamp", ""));

        ObjectNode normAlarm = mapper.createObjectNode();
        normAlarm.put("Id", id);
        normAlarm.put("State", state);
        normAlarm.put("Timestamp", ts);

        ObjectNode envelope = mapper.createObjectNode();
        envelope.put("CreatorId", creatorId);
        envelope.put("Type", "UpdateAlarm");
        envelope.put("Timestamp", ts);
        envelope.set("Alarm", normAlarm);
        String envJson = envelope.toString();

        String encrypted = EncryptProcessor.encryptToBase64(envJson);

        Map<String, Object> headers = new HashMap<>();
        headers.put("kafka.KEY", id);
        producer.sendBodyAndHeaders(
                "kafka:" + topic + "?valueSerializer=org.apache.kafka.common.serialization.StringSerializer&keySerializer=org.apache.kafka.common.serialization.StringSerializer",
                encrypted, headers);
        return true;
    }

    private static String textOr(JsonNode n, String field, String fallback) {
        JsonNode v = n.get(field);
        return (v == null || v.isNull()) ? fallback : v.asText(fallback);
    }

    private static JsonNode firstPresent(JsonNode n, String... fields) {
        for (String f : fields) {
            JsonNode v = n.get(f);
            if (v != null && !v.isMissingNode() && !v.isNull()) return v;
        }
        return null;
    }
}
