package com.pinkline.kafkabridge.model;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import java.util.List;

/**
 * Real TMS PAS info message format (topic: TMS.PISInfo):
 *
 * <rcsMsg>
 *   <hdr>
 *     <schema>rcs.e2k.ctc.train.pas.V1</schema>
 *     <sender>RCS.E2K.PIS</sender>
 *   </hdr>
 *   <data>
 *     <PlatformInfos>
 *       <PlatformInfo platform="PL1604">
 *         <StationId>16</StationId>
 *         <NextTrainApproacing>no</NextTrainApproacing>
 *         <Trains>
 *           <Train idx="0" id="0" GUID="trafficscheduler-..." name="02" tripno="3">
 *             <WillStop>yes</WillStop>
 *             <AtStation>no</AtStation>
 *             <LastForToday>no</LastForToday>
 *             <ArrivalTime>20260313T100921</ArrivalTime>
 *             <DepartureTime>20260313T100941</DepartureTime>
 *             <DestinationId>2502</DestinationId>
 *           </Train>
 *         </Trains>
 *       </PlatformInfo>
 *     </PlatformInfos>
 *   </data>
 * </rcsMsg>
 */
@JacksonXmlRootElement(localName = "rcsMsg")
public class RcsMsg {

    @JacksonXmlProperty(localName = "data")
    public Data data;

    public static class Data {
        @JacksonXmlProperty(localName = "PlatformInfos")
        public PlatformInfos platformInfos;
    }

    public static class PlatformInfos {
        @JacksonXmlElementWrapper(useWrapping = false)
        @JacksonXmlProperty(localName = "PlatformInfo")
        public List<PlatformInfo> platformInfo;
    }

    public static class PlatformInfo {
        // platform="PL1604" is an XML attribute on the element
        @JacksonXmlProperty(isAttribute = true, localName = "platform")
        public String platform;

        @JacksonXmlProperty(localName = "StationId")
        public int stationId;

        @JacksonXmlProperty(localName = "Trains")
        public Trains trains;
    }

    public static class Trains {
        @JacksonXmlElementWrapper(useWrapping = false)
        @JacksonXmlProperty(localName = "Train")
        public List<Train> train;
    }

    public static class Train {
        // All train identifiers are XML attributes on <Train>
        @JacksonXmlProperty(isAttribute = true, localName = "idx")    public int    idx;
        @JacksonXmlProperty(isAttribute = true, localName = "id")     public String id;
        @JacksonXmlProperty(isAttribute = true, localName = "GUID")   public String guid;
        @JacksonXmlProperty(isAttribute = true, localName = "name")   public String name;
        @JacksonXmlProperty(isAttribute = true, localName = "tripno") public int    tripNo;

        // Train state — child elements
        @JacksonXmlProperty(localName = "WillStop")      public String willStop;
        @JacksonXmlProperty(localName = "AtStation")     public String atStation;
        @JacksonXmlProperty(localName = "ArrivalTime")   public String arrivalTime;
        @JacksonXmlProperty(localName = "DepartureTime") public String departureTime;
        @JacksonXmlProperty(localName = "DestinationId") public String destinationId;
    }
}
