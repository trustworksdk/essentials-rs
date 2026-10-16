// Verifies exchange-simulator webhook signatures. Java 17+, JDK only.
// Run it to check it against a known signature: java Verify.java
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public class Verify {
    /**
     * True if {@code signature} (the X-Signature header) holds a valid HMAC-SHA256 of the raw body under
     * {@code secret}, and {@code timestamp} (the X-Signature-Timestamp header) is within {@code toleranceSeconds} of
     * {@code nowSeconds}. During the grace period after a secret rotation the header carries several comma-separated
     * values; one match is enough. Pass the body exactly as received, before any JSON parsing (in Spring:
     * {@code @RequestBody byte[] body}).
     */
    public static boolean verify(byte[] secret, String timestamp, String signature, byte[] rawBody, long nowSeconds,
                                 long toleranceSeconds) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(secret, "HmacSHA256"));
        mac.update((timestamp + ".").getBytes(StandardCharsets.US_ASCII));
        byte[] expected = ("sha256=" + HexFormat.of().formatHex(mac.doFinal(rawBody)))
                .getBytes(StandardCharsets.US_ASCII);
        boolean matches = false;
        for (String value : signature.split(",")) {
            matches |= MessageDigest.isEqual(expected, value.trim().getBytes(StandardCharsets.US_ASCII));
        }
        return matches && Math.abs(nowSeconds - Long.parseLong(timestamp)) <= toleranceSeconds;
    }

    public static boolean verify(byte[] secret, String timestamp, String signature, byte[] rawBody) throws Exception {
        return verify(secret, timestamp, signature, rawBody, Instant.now().getEpochSecond(), 300);
    }

    public static void main(String[] args) throws Exception {
        byte[] secret = "whsec_test_0123456789".getBytes(StandardCharsets.UTF_8);
        String timestamp = "1760443200";
        byte[] body = "{\"eventId\":\"e-1\"}".getBytes(StandardCharsets.UTF_8);
        String valid = "sha256=cde596a7a23fd7c03b27e1cf91e6fba42e15936f765508360d753019cd935bc1";
        long now = 1760443200L + 10;
        Map<String, Boolean> checks = new LinkedHashMap<>();
        checks.put("a valid signature", verify(secret, timestamp, valid, body, now, 300));
        checks.put("a rotation's second value", verify(secret, timestamp, "sha256=00, " + valid, body, now, 300));
        checks.put("not a changed body",
                !verify(secret, timestamp, valid, "{\"eventId\":\"e-1\"} ".getBytes(StandardCharsets.UTF_8), now, 300));
        checks.put("not another secret",
                !verify("another-secret-0123".getBytes(StandardCharsets.UTF_8), timestamp, valid, body, now, 300));
        checks.put("not an old timestamp", !verify(secret, timestamp, valid, body, now + 3600, 300));
        checks.forEach((name, ok) -> System.out.println((ok ? "ok    " : "FAIL  ") + name));
        System.exit(checks.values().stream().allMatch(ok -> ok) ? 0 : 1);
    }
}
