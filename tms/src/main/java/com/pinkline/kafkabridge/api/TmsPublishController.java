package com.pinkline.kafkabridge.api;

import org.apache.camel.ProducerTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.Set;

/**
 * TmsPublishController
 *
 * Lets the SCADA simulator dashboard inject test XML into the TMS-side
 * Artemis broker over HTTP, so a demo presenter can show the forward
 * path (TMS → SCADA) flowing without shelling out to artemis-pub.
 *
 *   POST /api/tms-publish/{topic}
 *     body: raw XML string (Content-Type: text/plain or application/xml)
 *     → publishes to activemq:topic:{topic}, picked up by the
 *       Connect tms-artemis-source connector and the rest of the chain.
 *
 * Topic must be one of the four configured Artemis topics (allow-list
 * to prevent abuse — anyone reaching this endpoint shouldn't be able to
 * write to arbitrary destinations).
 */
@RestController
@RequestMapping("/api/tms-publish")
@CrossOrigin("*")
public class TmsPublishController {

    private static final Logger log = LoggerFactory.getLogger(TmsPublishController.class);
    private static final Set<String> ALLOWED = Set.of(
            "TMS.PISInfo",
            "RCS.E2K.TMS.TrafficReportClient",
            "TSInfo",
            "RCS.E2K.TMS.RouteInfo");

    private final ProducerTemplate producer;

    public TmsPublishController(ProducerTemplate producer) {
        this.producer = producer;
    }

    @PostMapping(value = "/{topic}", consumes = MediaType.ALL_VALUE)
    public ResponseEntity<Map<String, Object>> publish(
            @PathVariable String topic,
            @RequestBody String body) {
        if (!ALLOWED.contains(topic)) {
            return ResponseEntity.badRequest().body(Map.of(
                    "ok", false,
                    "error", "topic not in allow-list",
                    "allowed", ALLOWED));
        }
        if (body == null || body.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of(
                    "ok", false,
                    "error", "empty body"));
        }
        producer.sendBody("activemq:topic:" + topic, body);
        log.info("TmsPublish — published {} chars to Artemis topic://{}", body.length(), topic);
        return ResponseEntity.ok(Map.of(
                "ok", true,
                "topic", topic,
                "size", body.length()));
    }

    @GetMapping("/topics")
    public Set<String> topics() {
        return ALLOWED;
    }
}
