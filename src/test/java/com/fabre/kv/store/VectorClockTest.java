package com.fabre.kv.store;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class VectorClockTest {

    @Test
    void incrementCreatesNewClockWithNodeIncremented() {
        VectorClock v = new VectorClock();
        VectorClock v1 = v.increment("http://node1");
        assertEquals(Map.of("http://node1", 1L), v1.getClock());
        VectorClock v2 = v1.increment("http://node1");
        assertEquals(Map.of("http://node1", 2L), v2.getClock());
    }

    @Test
    void mergeTakesMaxPerNode() {
        VectorClock a = new VectorClock(Map.of("n1", 1L, "n2", 2L));
        VectorClock b = new VectorClock(Map.of("n1", 2L, "n2", 1L));
        VectorClock m = a.merge(b);
        assertEquals(Map.of("n1", 2L, "n2", 2L), m.getClock());
    }

    @Test
    void compareBeforeAfter() {
        VectorClock a = new VectorClock(Map.of("n1", 1L));
        VectorClock b = new VectorClock(Map.of("n1", 2L));
        assertEquals(VectorClock.Order.BEFORE, a.compareTo(b));
        assertEquals(VectorClock.Order.AFTER, b.compareTo(a));
    }

    @Test
    void compareConcurrent() {
        VectorClock a = new VectorClock(Map.of("n1", 1L, "n2", 0L));
        VectorClock b = new VectorClock(Map.of("n1", 0L, "n2", 1L));
        assertEquals(VectorClock.Order.CONCURRENT, a.compareTo(b));
    }
}
