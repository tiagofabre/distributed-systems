package com.fabre.kv.hashring;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ConsistentHashRingTest {

    @Test
    void getNodesForReturnsUpToRNodes() {
        var ring = new ConsistentHashRing(List.of("http://n1", "http://n2", "http://n3"));
        List<String> nodes = ring.getNodesFor("key1", 2);
        assertEquals(2, nodes.size());
        assertTrue(nodes.contains("http://n1") || nodes.contains("http://n2") || nodes.contains("http://n3"));
    }

    @Test
    void getNodesForSameKeyReturnsSameOrder() {
        var ring = new ConsistentHashRing(List.of("http://n1", "http://n2", "http://n3"));
        List<String> a = ring.getNodesFor("key1", 3);
        List<String> b = ring.getNodesFor("key1", 3);
        assertEquals(a, b);
    }

    @Test
    void getAllNodesReturnsConfig() {
        var ring = new ConsistentHashRing(List.of("http://a", "http://b"));
        assertEquals(List.of("http://a", "http://b"), ring.getAllNodes());
    }
}
