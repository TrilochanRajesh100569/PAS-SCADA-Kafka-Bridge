package com.pinkline.kafkabridge.model;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import java.util.List;

/**
 * Real TMS XmlMessage format:
 * <XmlMessage>
 *   <PlatformPredictions><PlatPred>...</PlatPred></PlatformPredictions>
 *   <BlockOccupancies><BlOccu>...</BlOccu></BlockOccupancies>
 *   <VCIF><GateCMD>...</GateCMD></VCIF>
 * </XmlMessage>
 */
@JacksonXmlRootElement(localName = "XmlMessage")
public class XmlMessage {

    @JacksonXmlProperty(localName = "PlatformPredictions")
    public PlatformPredictions platformPredictions;

    @JacksonXmlProperty(localName = "BlockOccupancies")
    public BlockOccupancies blockOccupancies;

    @JacksonXmlProperty(localName = "VCIF")
    public Vcif vcif;

    public static class PlatformPredictions {
        @JacksonXmlElementWrapper(useWrapping = false)
        @JacksonXmlProperty(localName = "PlatPred")
        public List<PlatPred> platPred;
    }

    public static class PlatPred {
        @JacksonXmlProperty(localName = "PPId")  public String ppId;

        // Train slot 1
        @JacksonXmlProperty(localName = "PT1Id") public String pt1Id;
        @JacksonXmlProperty(localName = "PT1AT") public Integer pt1At;
        @JacksonXmlProperty(localName = "PT1DT") public String pt1Dt;
        @JacksonXmlProperty(localName = "PT1St") public Integer pt1St;
        @JacksonXmlProperty(localName = "PT1De") public String pt1De;
        @JacksonXmlProperty(localName = "PT1SS") public Integer pt1Ss;
        @JacksonXmlProperty(localName = "PT1RN") public Integer pt1Rn;

        // Train slot 2
        @JacksonXmlProperty(localName = "PT2Id") public String pt2Id;
        @JacksonXmlProperty(localName = "PT2AT") public Integer pt2At;
        @JacksonXmlProperty(localName = "PT2DT") public String pt2Dt;
        @JacksonXmlProperty(localName = "PT2St") public Integer pt2St;
        @JacksonXmlProperty(localName = "PT2De") public String pt2De;
        @JacksonXmlProperty(localName = "PT2SS") public Integer pt2Ss;
        @JacksonXmlProperty(localName = "PT2RN") public Integer pt2Rn;

        // Train slot 3
        @JacksonXmlProperty(localName = "PT3Id") public String pt3Id;
        @JacksonXmlProperty(localName = "PT3AT") public Integer pt3At;
        @JacksonXmlProperty(localName = "PT3DT") public String pt3Dt;
        @JacksonXmlProperty(localName = "PT3St") public Integer pt3St;
        @JacksonXmlProperty(localName = "PT3De") public String pt3De;
        @JacksonXmlProperty(localName = "PT3SS") public Integer pt3Ss;
        @JacksonXmlProperty(localName = "PT3RN") public Integer pt3Rn;
    }

    public static class BlockOccupancies {
        @JacksonXmlElementWrapper(useWrapping = false)
        @JacksonXmlProperty(localName = "BlOccu")
        public List<BlOccu> blOccu;
    }

    public static class BlOccu {
        @JacksonXmlProperty(localName = "BId") public Integer bId;
        @JacksonXmlProperty(localName = "BSt") public Integer bSt;
    }

    public static class Vcif {
        @JacksonXmlElementWrapper(useWrapping = false)
        @JacksonXmlProperty(localName = "GateCMD")
        public List<GateCMD> gateCMD;
    }

    public static class GateCMD {
        @JacksonXmlProperty(localName = "GId")  public Integer gId;
        @JacksonXmlProperty(localName = "GCmd") public Integer gCmd;
    }
}
