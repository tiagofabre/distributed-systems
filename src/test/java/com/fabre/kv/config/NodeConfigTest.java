package com.fabre.kv.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("NodeConfig")
class NodeConfigTest {

    @Test
    @DisplayName("parseNodesFromString applies ensureScheme to each URL (regression)")
    void parseNodesFromStringAppliesEnsureScheme() {
        String nodesStr = "http://127.0.0.1:8201,://127.0.0.1:8202,http://127.0.0.1:8203";
        List<String> parsed = NodeConfig.parseNodesFromString(nodesStr);
        assertEquals(List.of("http://127.0.0.1:8201", "http://127.0.0.1:8202", "http://127.0.0.1:8203"), parsed);
    }

    @Test
    @DisplayName("parseNodesFromString tolerates spaces around commas")
    void parseNodesFromStringToleratesSpaces() {
        List<String> parsed = NodeConfig.parseNodesFromString("http://a , http://b , http://c");
        assertEquals(List.of("http://a", "http://b", "http://c"), parsed);
    }

    @Test
    @DisplayName("parseNodesFromString returns empty list for null or blank")
    void parseNodesFromStringReturnsEmptyForNullOrBlank() {
        assertTrue(NodeConfig.parseNodesFromString(null).isEmpty());
        assertTrue(NodeConfig.parseNodesFromString("").isEmpty());
        assertTrue(NodeConfig.parseNodesFromString("   ").isEmpty());
    }

    @Test
    @DisplayName("build throws when writeQuorum exceeds replicationFactor")
    void buildThrowsWhenWriteQuorumExceedsReplicationFactor() {
        assertThrows(IllegalArgumentException.class, () ->
                NodeConfig.builder().replicationFactor(3).writeQuorum(10).build());
    }

    @Test
    @DisplayName("build throws when readQuorum exceeds replicationFactor")
    void buildThrowsWhenReadQuorumExceedsReplicationFactor() {
        assertThrows(IllegalArgumentException.class, () ->
                NodeConfig.builder().replicationFactor(3).readQuorum(5).build());
    }

    @Test
    @DisplayName("build throws when replicationFactor is zero")
    void buildThrowsWhenReplicationFactorZero() {
        assertThrows(IllegalArgumentException.class, () ->
                NodeConfig.builder().replicationFactor(0).build());
    }
}
