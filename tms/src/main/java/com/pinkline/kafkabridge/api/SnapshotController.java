package com.pinkline.kafkabridge.api;

import com.pinkline.kafkabridge.config.BridgeConfig;
import org.apache.camel.ProducerTemplate;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.*;

/**
 * SnapshotController
 *
 * Implements the on-link-up SNAPSHOT replay described in the alarm
 * flood-control note. POST /api/snapshot/replay reads the compacted
 * alarm-state topic from the earliest offset, ending up with one
 * record per alarmId (the latest state thanks to log compaction),
 * and republishes each to RabbitMQ amq.topic so SCADA receives a
 * consolidated snapshot instead of the full event history.
 *
 * Caller (e.g. SCADA reconnect coordinator):
 *   POST /api/snapshot/replay
 *   → 200 { "alarms": 15, "topic": "scada.tms.alarms.state" }
 */
@RestController
@RequestMapping("/api/snapshot")
@CrossOrigin("*")
public class SnapshotController {

    private static final Logger log = LoggerFactory.getLogger(SnapshotController.class);

    private final BridgeConfig config;
    private final ProducerTemplate producer;

    @Value("${kafka.brokers:kafka-service:9092}")
    private String kafkaBrokers;

    public SnapshotController(BridgeConfig config, ProducerTemplate producer) {
        this.config = config;
        this.producer = producer;
    }

    @PostMapping("/replay")
    public ResponseEntity<Map<String, Object>> replay() {
        BridgeConfig.AlarmState as = config.getAlarmState();
        if (!as.isEnabled()) {
            return ResponseEntity.status(409).body(Map.of(
                    "error", "alarm-state fan-out disabled",
                    "hint",  "set bridge.alarm-state.enabled=true to enable"));
        }

        BridgeConfig.RabbitmqOut rmq = config.getRabbitmqOut();
        String topic = as.getTopic();

        // Read everything from the compacted topic, end-to-end, and keep only
        // the latest record per key. Compaction is best-effort, so duplicates
        // can still appear if cleanup hasn't run; the in-memory map collapses
        // them on the consumer side too.
        Map<String, String> latest = new LinkedHashMap<>();
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "snapshot-replay-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> parts = new ArrayList<>();
            consumer.partitionsFor(topic).forEach(p ->
                    parts.add(new TopicPartition(topic, p.partition())));
            consumer.assign(parts);
            consumer.seekToBeginning(parts);

            Map<TopicPartition, Long> ends = consumer.endOffsets(parts);
            int idle = 0;
            while (idle < 3) {
                ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(500));
                if (recs.isEmpty()) {
                    idle++;
                    continue;
                }
                idle = 0;
                for (ConsumerRecord<String, String> r : recs) {
                    if (r.key() == null) continue;
                    if (r.value() == null) {
                        latest.remove(r.key());   // tombstone
                    } else {
                        latest.put(r.key(), r.value());
                    }
                }
                // Stop when we've consumed past the snapshot of end offsets.
                boolean done = true;
                for (TopicPartition tp : parts) {
                    if (consumer.position(tp) < ends.getOrDefault(tp, 0L)) {
                        done = false;
                        break;
                    }
                }
                if (done) break;
            }
        }

        String uri = "spring-rabbitmq:" + rmq.getExchange()
                + "?routingKey=" + rmq.getRoutingKey();
        for (Map.Entry<String, String> e : latest.entrySet()) {
            producer.sendBody(uri, e.getValue());
        }
        log.info("Snapshot replay — published {} alarm states from {} to {}",
                latest.size(), topic, rmq.getExchange());

        return ResponseEntity.ok(Map.of(
                "alarms", latest.size(),
                "topic",  topic,
                "sinkExchange", rmq.getExchange(),
                "routingKey",   rmq.getRoutingKey()));
    }
}
