package com.pinkline.kafkabridge;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pinkline.kafkabridge.processor.XmlToJsonProcessor;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that each real TMS XML format is correctly converted to ICD JSON
 * by Jackson automatically — no manual parsing.
 *
 * Run: mvn test
 * No Artemis, Kafka, or RabbitMQ needed.
 */
class XmlToJsonProcessorTest {

    private static final ObjectMapper json = new ObjectMapper();
    private XmlToJsonProcessor processor;
    private DefaultCamelContext ctx;

    @BeforeEach
    void setup() {
        processor = new XmlToJsonProcessor();
        ctx = new DefaultCamelContext();
    }

    // ── ATRTimeTable ──────────────────────────────────────────────────────
    @Test
    void testATRTimeTable_convertsToIcdJson() throws Exception {

        String xml =
            "<ATRTimeTable>" +
            "  <dateTime>20260411T140000</dateTime>" +
            "  <StartIndex>0</StartIndex>" +
            "  <TotalCount>1</TotalCount>" +
            "  <GraphID>42</GraphID>" +
            "  <Trains>" +
            "    <Tg>" +
            "      <TTGUID>dc-occ.eclrt-train-6250-guid</TTGUID>" +
            "      <Name>PinkLine-A</Name>" +
            "      <TripNo>678</TripNo>" +
            "      <CTD lpid=\"101\" tn=\"678\"/>" +
            "      <TmsPTI trguid=\"dc-occ.eclrt-train-6250-guid\" typeid=\"1\" ttypeid=\"2\"/>" +
            "      <Evts F=\"3\" Id=\"2201\" As=\"3600\" Ds=\"3660\"/>" +
            "    </Tg>" +
            "  </Trains>" +
            "</ATRTimeTable>";

        Exchange exchange = process(xml, "RCS.E2K.TMS.ATRTimeTableMsg.V3");
        JsonNode root = json.readTree(exchange.getIn().getBody(String.class));

        // ICD envelope
        assertEquals("1.0",             root.get("schemaVersion").asText());
        assertEquals("TMS_PAS_UPDATE",  root.get("messageType").asText());
        assertEquals("RCS.E2K.PIS",     root.get("header").get("server").asText());
        assertNotNull(root.get("timestamp"));

        // Platform prediction
        JsonNode platforms = root.get("platformPredictions");
        assertEquals(1, platforms.size());
        assertEquals("PL2201", platforms.get(0).get("platformId").asText());

        // Train data
        JsonNode train = platforms.get(0).get("predictedTrains").get(0);
        assertEquals(1,     train.get("slot").asInt());
        assertEquals(678,   train.get("runNumber").asInt());
        assertEquals("PL101", train.get("destination").asText());
        assertEquals(1,     train.get("serviceState").asInt());

        System.out.println("ATRTimeTable → JSON:");
        System.out.println(json.writerWithDefaultPrettyPrinter()
                .writeValueAsString(root));
    }

    // ── SingleArrival ─────────────────────────────────────────────────────
    @Test
    void testSingleArrival_convertsToIcdJson() throws Exception {

        String xml =
            "<SingleArrival>" +
            "  <Arr>" +
            "    <train>" +
            "      <TTGUID>dc-occ.eclrt-train-6250-guid</TTGUID>" +
            "    </train>" +
            "    <loc><tmsid>2201</tmsid></loc>" +
            "    <atimes><oTime>20500411T143000</oTime></atimes>" +
            "  </Arr>" +
            "</SingleArrival>";

        Exchange exchange = process(xml, "RCS.E2K.TMS.TrafficReportClient.SingleArrival.V2");
        JsonNode root = json.readTree(exchange.getIn().getBody(String.class));

        assertEquals("TMS_PAS_UPDATE", root.get("messageType").asText());

        JsonNode platform = root.get("platformPredictions").get(0);
        assertEquals("PL2201", platform.get("platformId").asText());

        JsonNode train = platform.get("predictedTrains").get(0);
        assertEquals("dc-occ.eclrt-train-6250-guid", train.get("trainId").asText());
        assertEquals(1, train.get("status").asInt());   // 1 = at platform
        assertTrue(train.get("arrivalTime").asLong() > 0);

        System.out.println("SingleArrival → JSON:");
        System.out.println(json.writerWithDefaultPrettyPrinter()
                .writeValueAsString(root));
    }

    // ── SingleDeparture ───────────────────────────────────────────────────
    @Test
    void testSingleDeparture_convertsToIcdJson() throws Exception {

        String xml =
            "<SingleDeparture>" +
            "  <Dep>" +
            "    <train>" +
            "      <TTGUID>dc-occ.eclrt-train-6250-guid</TTGUID>" +
            "    </train>" +
            "    <loc><tmsid>2201</tmsid></loc>" +
            "    <dtimes><oTime>20260411T143300</oTime></dtimes>" +
            "  </Dep>" +
            "</SingleDeparture>";

        Exchange exchange = process(xml, "RCS.E2K.TMS.TrafficReportClient.SingleDeparture.V2");
        JsonNode root = json.readTree(exchange.getIn().getBody(String.class));

        assertEquals("TMS_PAS_UPDATE", root.get("messageType").asText());

        JsonNode platform = root.get("platformPredictions").get(0);
        assertEquals("PL2201", platform.get("platformId").asText());

        JsonNode train = platform.get("predictedTrains").get(0);
        assertEquals(2, train.get("status").asInt());   // 2 = departed
        assertFalse(train.get("departureTime").asText().isBlank());

        System.out.println("SingleDeparture → JSON:");
        System.out.println(json.writerWithDefaultPrettyPrinter()
                .writeValueAsString(root));
    }

    // ── RouteInfo ─────────────────────────────────────────────────────────
    @Test
    void testRouteInfo_convertsToIcdJson() throws Exception {

        String xml =
            "<routeinfo>" +
            "  <TTGUID>dc-occ.eclrt-train-6250-guid</TTGUID>" +
            "  <dests>" +
            "    <dest tmsid=\"2201\"/>" +
            "    <dest tmsid=\"2202\"/>" +
            "    <dest tmsid=\"2203\"/>" +
            "  </dests>" +
            "</routeinfo>";

        Exchange exchange = process(xml, "RCS.E2K.TMS.RouteInfo.V2");
        JsonNode root = json.readTree(exchange.getIn().getBody(String.class));

        assertEquals("TMS_PAS_UPDATE", root.get("messageType").asText());

        JsonNode route = root.get("routeInfo");
        assertNotNull(route);
        assertEquals("dc-occ.eclrt-train-6250-guid", route.get("trainGUID").asText());
        assertEquals(3, route.get("route").size());
        assertEquals("PL2201", route.get("route").get(0).asText());
        assertEquals("PL2202", route.get("route").get(1).asText());

        System.out.println("RouteInfo → JSON:");
        System.out.println(json.writerWithDefaultPrettyPrinter()
                .writeValueAsString(root));
    }

    // ── History events skipped ────────────────────────────────────────────
    @Test
    void testATRTimeTable_historyEventsSkipped() throws Exception {

        String xml =
            "<ATRTimeTable>" +
            "  <dateTime>20260411T140000</dateTime>" +
            "  <StartIndex>0</StartIndex><TotalCount>1</TotalCount><GraphID>1</GraphID>" +
            "  <Trains>" +
            "    <Tg>" +
            "      <TTGUID>train-history</TTGUID>" +
            "      <TripNo>1</TripNo>" +
            "      <Evts F=\"7\" Id=\"2201\" As=\"100\" Ds=\"200\"/>" + // F=7 includes 0x04=HISTORY
            "    </Tg>" +
            "  </Trains>" +
            "</ATRTimeTable>";

        Exchange exchange = process(xml, "RCS.E2K.TMS.ATRTimeTableMsg.V3");
        JsonNode root = json.readTree(exchange.getIn().getBody(String.class));

        // History item should be skipped — platformPredictions should be empty
        assertEquals(0, root.get("platformPredictions").size(),
            "History events must be skipped");

        System.out.println("History events correctly skipped");
    }

    // ── Helper ────────────────────────────────────────────────────────────
    private Exchange process(String xml, String schema) throws Exception {
        Exchange exchange = new DefaultExchange(ctx);
        exchange.getIn().setBody(xml);
        exchange.getIn().setHeader("rcsschema", schema);
        processor.process(exchange);
        return exchange;
    }
}
