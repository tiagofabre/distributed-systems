package com.fabre.kv.integration;

import com.fabre.kv.KvNodeApplication;
import com.fabre.kv.config.NodeConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test: starts 3 nodes in-process, PUT on node1, then GET from
 * node2 and assert the value was replicated.
 */
@Tag("integration")
@DisplayName("Replication sync with 3 nodes in-process")
class ReplicationSyncTest {

    private static final String KEY = "replication-sync-test-key";
    private static final String VALUE = "synced";
    private static final int REPLICATION_WAIT_MS = 2_000;
    private static final int PORT_WAIT_MS = 15_000;
    private static final int PORT_POLL_MS = 100;

    private static final List<String> NODES = List.of("http://127.0.0.1:8201", "http://127.0.0.1:8202",
            "http://127.0.0.1:8203");

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    private static KvNodeApplication.NodeRunner runner1;
    private static KvNodeApplication.NodeRunner runner2;
    private static KvNodeApplication.NodeRunner runner3;

    @BeforeAll
    @DisplayName("start 3 KV nodes and wait until they are listening")
    static void startNodes() throws Exception {
        NodeConfig config1 = NodeConfig.builder().port(8201).nodeUrl(NODES.get(0)).nodes(NODES).build();
        NodeConfig config2 = NodeConfig.builder().port(8202).nodeUrl(NODES.get(1)).nodes(NODES).build();
        NodeConfig config3 = NodeConfig.builder().port(8203).nodeUrl(NODES.get(2)).nodes(NODES).build();

        runner1 = KvNodeApplication.startWithConfig(config1);
        runner2 = KvNodeApplication.startWithConfig(config2);
        runner3 = KvNodeApplication.startWithConfig(config3);

        TimeUnit.MILLISECONDS.sleep(500);
        waitForPort(8201);
        waitForPort(8202);
        waitForPort(8203);
    }

    private static void waitForPort(int port) throws InterruptedException {
        long deadline = System.currentTimeMillis() + PORT_WAIT_MS;
        while (System.currentTimeMillis() < deadline) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress("127.0.0.1", port), 500);
                return;
            } catch (Exception ignored) {
            }
            TimeUnit.MILLISECONDS.sleep(PORT_POLL_MS);
        }
        throw new AssertionError("Node on port " + port + " did not become ready within " + PORT_WAIT_MS + " ms");
    }

    /**
     * Sends PUT request; on 503 retries up to maxRetries times with delayMs between
     * attempts. Returns last status.
     */
    private static int sendPutWithRetry(HttpRequest putRequest, int maxRetries, int delayMs) throws Exception {
        int lastStatus = 0;
        for (int i = 0; i < maxRetries; i++) {
            HttpResponse<String> resp = HTTP.send(putRequest,
                    HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            lastStatus = resp.statusCode();
            if (lastStatus != 503)
                return lastStatus;
            if (i < maxRetries - 1)
                TimeUnit.MILLISECONDS.sleep(delayMs);
        }
        return lastStatus;
    }

    @Nested
    @DisplayName("when a value is PUT to node1 (8201)")
    class PutOnNode1 {

        @Test
        @DisplayName("PUT on 8201 returns 204")
        void putOnNode1() throws Exception {

            String putUrl = NODES.get(0) + "/kv/" + KEY;
            String putBody = "{\"value\":\"" + VALUE + "\"}";

            HttpRequest putReq = HttpRequest.newBuilder().uri(URI.create(putUrl)).timeout(Duration.ofSeconds(10))
                    .header("Content-Type", "application/json")
                    .PUT(HttpRequest.BodyPublishers.ofString(putBody, StandardCharsets.UTF_8)).build();
            HttpResponse<String> putResp = HTTP.send(putReq,
                    HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

            assertEquals(204, putResp.statusCode(), "PUT must return 204");

            TimeUnit.MILLISECONDS.sleep(REPLICATION_WAIT_MS);
        }

        @Test
        @DisplayName("GET from node2 (8202) returns the same value after replication")
        void getFromNode2ReturnsSameValue() throws Exception {
            String getUrl = NODES.get(1) + "/kv/" + KEY;
            HttpRequest getReq = HttpRequest.newBuilder().uri(URI.create(getUrl)).timeout(Duration.ofSeconds(10)).GET()
                    .build();
            HttpResponse<String> getResp = HTTP.send(getReq,
                    HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

            assertEquals(200, getResp.statusCode(), "GET must return 200");

            JsonNode body = JSON.readTree(getResp.body());
            assertTrue(body.has("value"), "GET response must contain 'value'");
            assertEquals(VALUE, body.get("value").asText(), "GET must return the value written by PUT");
            assertTrue(body.has("clock"), "GET response must contain 'clock'");
        }
    }

    @Nested
    @DisplayName("when the same key is updated with a second PUT")
    class KeyUpdateScenario {

        private static final String UPDATE_KEY = "demo-key";
        private static final String FIRST_VALUE = "hello-from-node1";
        private static final String SECOND_VALUE = "hello-from-node4";

        @Test
        @DisplayName("second PUT updates the value; GET from node2 returns the latest value")
        void secondPutUpdatesKeyGetFromNode2ReturnsLatest() throws Exception {
            String putUrl = NODES.get(0) + "/kv/" + UPDATE_KEY;
            String getUrl = NODES.get(1) + "/kv/" + UPDATE_KEY;

            // First PUT
            HttpRequest put1 = HttpRequest.newBuilder().uri(URI.create(putUrl)).timeout(Duration.ofSeconds(10))
                    .header("Content-Type", "application/json").PUT(HttpRequest.BodyPublishers
                            .ofString("{\"value\":\"" + FIRST_VALUE + "\"}", StandardCharsets.UTF_8))
                    .build();
            HttpResponse<String> resp1 = HTTP.send(put1, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            assertEquals(204, resp1.statusCode(), "First PUT must return 204");

            TimeUnit.MILLISECONDS.sleep(REPLICATION_WAIT_MS);

            // GET from node2: must see first value
            HttpRequest get1 = HttpRequest.newBuilder().uri(URI.create(getUrl)).timeout(Duration.ofSeconds(10)).GET()
                    .build();
            HttpResponse<String> getResp1 = HTTP.send(get1, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            assertEquals(200, getResp1.statusCode(), "First GET must return 200");
            JsonNode body1 = JSON.readTree(getResp1.body());
            assertEquals(FIRST_VALUE, body1.get("value").asText(), "First GET must return first value");

            // Second PUT (update same key). Retry on 503 (write quorum) to tolerate
            // replication timing.
            HttpRequest put2 = HttpRequest.newBuilder().uri(URI.create(putUrl)).timeout(Duration.ofSeconds(10))
                    .header("Content-Type", "application/json").PUT(HttpRequest.BodyPublishers
                            .ofString("{\"value\":\"" + SECOND_VALUE + "\"}", StandardCharsets.UTF_8))
                    .build();
            int put2Status = sendPutWithRetry(put2, 3, 500);
            assertEquals(204, put2Status,
                    "Second PUT must return 204 (after retries if quorum was transiently unreached)");

            TimeUnit.MILLISECONDS.sleep(REPLICATION_WAIT_MS);

            // GET from node2 again: must see updated value, not the first
            HttpRequest get2 = HttpRequest.newBuilder().uri(URI.create(getUrl)).timeout(Duration.ofSeconds(10)).GET()
                    .build();
            HttpResponse<String> getResp2 = HTTP.send(get2, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            assertEquals(200, getResp2.statusCode(), "Second GET must return 200");
            JsonNode body2 = JSON.readTree(getResp2.body());
            assertEquals(SECOND_VALUE, body2.get("value").asText(),
                    "Second GET must return the updated value (key was updated by second PUT)");
        }
    }

    @AfterAll
    @DisplayName("stop all nodes")
    static void stopNodes() {
        if (runner1 != null)
            runner1.stop();
        if (runner2 != null)
            runner2.stop();
        if (runner3 != null)
            runner3.stop();
    }
}
