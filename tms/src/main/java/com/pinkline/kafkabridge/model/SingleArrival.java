package com.pinkline.kafkabridge.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

/**
 * SingleArrival — XML model for topic RCS.E2K.TMS.TrafficReportClient
 * Schema: RCS.E2K.TMS.TrafficReportClient.SingleArrival.V2
 */
@JacksonXmlRootElement(localName = "SingleArrival")
@JsonIgnoreProperties(ignoreUnknown = true)
public class SingleArrival {

    @JacksonXmlProperty(localName = "Arr")
    public Arr arr;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Arr {

        @JacksonXmlProperty(localName = "train")
        public Train train;

        @JacksonXmlProperty(localName = "loc")
        public Loc loc;

        @JacksonXmlProperty(localName = "atimes")
        public Times atimes;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Train {
        @JacksonXmlProperty(localName = "TTGUID")
        public String ttGuid;

        @JacksonXmlProperty(localName = "id")
        public String id;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Loc {
        @JacksonXmlProperty(localName = "tmsid")
        public int tmsId;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Times {
        @JacksonXmlProperty(localName = "oTime")
        public String oTime;
    }
}
