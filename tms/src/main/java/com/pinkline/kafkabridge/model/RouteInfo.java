package com.pinkline.kafkabridge.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import java.util.List;

/**
 * RouteInfo — XML model for topic RCS.E2K.TMS.RouteInfo
 * Schema: RCS.E2K.TMS.RouteInfo.V2
 */
@JacksonXmlRootElement(localName = "routeinfo")
@JsonIgnoreProperties(ignoreUnknown = true)
public class RouteInfo {

    @JacksonXmlProperty(localName = "TTGUID")
    public String ttGuid;

    @JacksonXmlProperty(localName = "trainid")
    public String trainId;

    @JacksonXmlProperty(localName = "dests")
    public Dests dests;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Dests {
        @JacksonXmlElementWrapper(useWrapping = false)
        @JacksonXmlProperty(localName = "dest")
        public List<Dest> dest;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Dest {
        @JacksonXmlProperty(isAttribute = true, localName = "tmsid")
        public int tmsId;
    }
}
