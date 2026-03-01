package com.fabre.kv.replication;

import com.fabre.kv.store.VectorClock;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("ReplicationClient")
class ReplicationClientTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    @DisplayName("jsonPutBodyForTest produces valid JSON when clock keys contain quotes and newlines")
    void jsonPutBodyEscapesClockKeys() {
        VectorClock clock = new VectorClock(
                Map.of("http://node\nwith\nnewlines", 1L, "key\"with\"quotes", 2L));
        String json = ReplicationClient.jsonPutBodyForTest("value", clock);

        assertDoesNotThrow(() -> JSON.readTree(json));
        assertTrue(json.contains("value"));
        assertTrue(json.contains("clock"));
        assertTrue(json.contains("\\n") || json.contains("\\\\n"));
        assertTrue(json.contains("\\\""));
    }
}
