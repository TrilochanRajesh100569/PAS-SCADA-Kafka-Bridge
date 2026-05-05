package com.pinkline.kafkabridge.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JsonToXmlProcessor
 *
 * Reverse of XmlToJsonProcessor — converts RSAE JSON from SCADA API into XML
 * so TMS applications that expect XML on Artemis topics can consume it directly.
 *
 * Config: bridge.inbound[n].convert-to-xml=true   (default: false = pass JSON as-is)
 *
 * Input (from ScadaInboundProcessor):
 *   { "type": "UpdateAlarm", "creatorId": "ScateX", "timestamp": "...", "alarms": [...] }
 *
 * Output XML:
 *   <RSAEMessage>
 *     <type>UpdateAlarm</type>
 *     <creatorId>ScateX</creatorId>
 *     <timestamp>...</timestamp>
 *     <alarms>...</alarms>
 *   </RSAEMessage>
 */
public class JsonToXmlProcessor implements Processor {

    private static final Logger log = LoggerFactory.getLogger(JsonToXmlProcessor.class);
    private static final ObjectMapper jsonMapper = new ObjectMapper();
    private static final XmlMapper xmlMapper = new XmlMapper();

    @Override
    public void process(Exchange exchange) throws Exception {
        String json = exchange.getIn().getBody(String.class);

        if (json == null || json.isBlank()) {
            log.warn("JsonToXml — empty input, skipping conversion");
            return;
        }

        try {
            JsonNode root = jsonMapper.readTree(json);
            // Wrap in RSAEMessage root element and serialize to XML
            String xml = xmlMapper.writer()
                    .withRootName("RSAEMessage")
                    .writeValueAsString(root);

            exchange.getIn().setBody(xml);
            exchange.getIn().setHeader("Content-Type", "application/xml");
            log.debug("JsonToXml — converted RSAE JSON to XML ({} chars)", xml.length());

        } catch (Exception e) {
            log.error("JsonToXml — conversion failed, forwarding raw JSON: {}", e.getMessage());
            // Non-fatal: pass JSON as-is rather than dropping the message
        }
    }
}
