package com.pinkline.kafkabridge.processor;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * EncryptProcessor
 *
 * Encrypts the JSON string (from XmlToJsonProcessor) using AES-256-GCM.
 *
 * Key source: Environment variable SCADA_AES_KEY (Base64-encoded 256-bit key)
 *             NEVER from config files or hardcoded.
 *
 * Wire format output (byte[]):
 *   [0  - 11]  : 12-byte IV  (random, fresh per message)
 *   [12 - n-16]: Ciphertext  (encrypted JSON bytes)
 *   [n-16 - n] : GCM Tag     (16-byte authentication tag — appended by JCE)
 *
 * Input  (Exchange body): JSON String
 * Output (Exchange body): byte[]
 */
public class EncryptProcessor implements Processor {

    private static final Logger log = LoggerFactory.getLogger(EncryptProcessor.class);

    private static final String ENV_KEY     = "SCADA_AES_KEY";
    private static final int    IV_LENGTH   = 12;   // 12 bytes for GCM
    private static final int    GCM_TAG_BIT = 128;  // 128-bit = 16-byte GCM tag

    @Override
    public void process(Exchange exchange) throws Exception {
        String jsonBody = exchange.getIn().getBody(String.class);
        String encoded = encryptToBase64(jsonBody);
        exchange.getIn().setBody(encoded);
        log.debug("EncryptProcessor — encrypted {} chars of JSON -> {} char base64",
                jsonBody.length(), encoded.length());
    }

    /**
     * Encrypts a JSON string and returns the base64-encoded wire-format payload.
     * Reusable from non-Camel callers (REST controllers, fan-out processors).
     *
     * Wire format: [12B IV][ciphertext+16B GCM tag], then base64 encoded.
     */
    public static String encryptToBase64(String jsonBody) throws Exception {
        String keyB64 = System.getenv(ENV_KEY);
        if (keyB64 == null || keyB64.isBlank()) {
            throw new IllegalStateException(
                "SCADA_AES_KEY environment variable is not set! " +
                "Set it on the server: export SCADA_AES_KEY=<base64-256bit-key>");
        }
        byte[] keyBytes = Base64.getDecoder().decode(keyB64.trim());
        if (keyBytes.length != 32) {
            throw new IllegalStateException(
                "SCADA_AES_KEY must decode to exactly 32 bytes (256-bit). " +
                "Current length: " + keyBytes.length);
        }
        SecretKey secretKey = new SecretKeySpec(keyBytes, "AES");

        byte[] iv = new byte[IV_LENGTH];
        SecureRandom.getInstanceStrong().nextBytes(iv);

        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.ENCRYPT_MODE, secretKey,
                new GCMParameterSpec(GCM_TAG_BIT, iv));
        byte[] ciphertext = cipher.doFinal(jsonBody.getBytes(StandardCharsets.UTF_8));

        byte[] payload = new byte[IV_LENGTH + ciphertext.length];
        System.arraycopy(iv,         0, payload, 0,         IV_LENGTH);
        System.arraycopy(ciphertext, 0, payload, IV_LENGTH, ciphertext.length);
        return Base64.getEncoder().encodeToString(payload);
    }
}
