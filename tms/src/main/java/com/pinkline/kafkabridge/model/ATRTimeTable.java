package com.pinkline.kafkabridge.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import java.util.List;

/**
 * ATRTimeTable — XML model for topic TMS.PISInfo
 * Schema: RCS.E2K.TMS.ATRTimeTableMsg.V3
 */
@JacksonXmlRootElement(localName = "ATRTimeTable")
@JsonIgnoreProperties(ignoreUnknown = true)
public class ATRTimeTable {

    @JacksonXmlProperty(localName = "dateTime")
    public String dateTime;

    @JacksonXmlProperty(localName = "Trains")
    public Trains trains;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Trains {
        @JacksonXmlElementWrapper(useWrapping = false)
        @JacksonXmlProperty(localName = "Tg")
        public List<Tg> tg;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Tg {

        @JacksonXmlProperty(localName = "Id")
        public String id;

        @JacksonXmlProperty(localName = "TTGUID")
        public String ttGuid;

        @JacksonXmlProperty(localName = "TripNo")
        public int tripNo;

        @JacksonXmlProperty(localName = "CTD")
        public CTD ctd;

        @JacksonXmlProperty(localName = "TmsPTI")
        public TmsPTI tmsPti;

        @JacksonXmlElementWrapper(useWrapping = false)
        @JacksonXmlProperty(localName = "Evts")
        public List<Evts> evts;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CTD {
        @JacksonXmlProperty(isAttribute = true, localName = "lpid")
        public int lpid;

        @JacksonXmlProperty(isAttribute = true, localName = "tn")
        public int tn;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TmsPTI {
        @JacksonXmlProperty(isAttribute = true, localName = "trguid")
        public String trGuid;

        @JacksonXmlProperty(isAttribute = true, localName = "typeid")
        public int typeId;

        @JacksonXmlProperty(isAttribute = true, localName = "ttypeid")
        public int tripTypeId;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Evts {
        // Flags bitmask: 0x01=ARRIVAL_VALID, 0x02=DEPARTURE_VALID,
        //                0x04=HISTORY_ITEM (skip), 0x20=PASS_EVENT (skip)
        @JacksonXmlProperty(isAttribute = true, localName = "F")
        public int flags;

        @JacksonXmlProperty(isAttribute = true, localName = "Id")
        public int platformTmsId;

        @JacksonXmlProperty(isAttribute = true, localName = "As")
        public int arrivalSecs;

        @JacksonXmlProperty(isAttribute = true, localName = "Ds")
        public int departureSecs;
    }
}
