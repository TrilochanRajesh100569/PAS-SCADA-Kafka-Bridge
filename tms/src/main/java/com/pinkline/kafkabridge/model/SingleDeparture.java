package com.pinkline.kafkabridge.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

/**
 * SingleDeparture — XML model for topic RCS.E2K.TMS.TrafficReportClient
 * Schema: RCS.E2K.TMS.TrafficReportClient.SingleDeparture.V2
 */
@JacksonXmlRootElement(localName = "SingleDeparture")
@JsonIgnoreProperties(ignoreUnknown = true)
public class SingleDeparture {

    @JacksonXmlProperty(localName = "Dep")
    public Dep dep;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Dep {

        @JacksonXmlProperty(localName = "train")
        public Train train;

        @JacksonXmlProperty(localName = "loc")
        public Loc loc;

        @JacksonXmlProperty(localName = "dtimes")
        public Times dtimes;
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
