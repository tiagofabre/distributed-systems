package com.fabre.kv.store;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryStoreTest {

    @Test
    void getReturnsEmptyWhenMissing() {
        InMemoryStore store = new InMemoryStore();
        assertTrue(store.get("x").isEmpty());
    }

    @Test
    void putAndGet() {
        InMemoryStore store = new InMemoryStore();
        VectorClock clock = new VectorClock().increment("http://n1");
        store.put("k", "v", clock);
        var opt = store.get("k");
        assertTrue(opt.isPresent());
        assertEquals("v", opt.get().value());
        assertEquals(clock.getClock(), opt.get().clock().getClock());
    }

    @Test
    void putWithIncrementStoresIncrementedClock() {
        InMemoryStore store = new InMemoryStore();
        store.put("k", "v", null, "http://n1");
        var opt = store.get("k");
        assertTrue(opt.isPresent());
        assertEquals("v", opt.get().value());
        assertEquals(Map.of("http://n1", 1L), opt.get().clock().getClock());
    }
}
