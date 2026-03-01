package com.fabre.kv.store;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Immutable vector clock for versioning values across replicas. Keys are node URLs; values are logical timestamps.
 */
public final class VectorClock {

    public enum Order {
        BEFORE, // this < other
        AFTER, // this > other
        CONCURRENT, EQUAL
    }

    private final Map<String, Long> clock;

    public VectorClock() {
        this.clock = Map.of();
    }

    public VectorClock(Map<String, Long> clock) {
        this.clock = clock.isEmpty() ? Map.of() : Collections.unmodifiableMap(new HashMap<>(clock));
    }

    public Map<String, Long> getClock() {
        return clock;
    }

    /**
     * Returns a new vector clock with the given node's counter incremented by one.
     */
    public VectorClock increment(String nodeUrl) {
        Map<String, Long> next = new HashMap<>(clock);
        next.merge(nodeUrl, 1L, Long::sum);
        return new VectorClock(next);
    }

    /**
     * Merge with another clock: take the maximum value for each node.
     */
    public VectorClock merge(VectorClock other) {
        if (other == null || other.clock.isEmpty())
            return this;
        if (clock.isEmpty())
            return other;
        Map<String, Long> merged = new HashMap<>(clock);
        for (Map.Entry<String, Long> e : other.clock.entrySet()) {
            merged.merge(e.getKey(), e.getValue(), Math::max);
        }
        return new VectorClock(merged);
    }

    /**
     * Compare this clock with another.
     */
    public Order compareTo(VectorClock other) {
        if (other == null)
            return Order.AFTER;
        if (clock.isEmpty() && other.clock.isEmpty())
            return Order.EQUAL;
        if (clock.isEmpty())
            return Order.BEFORE;
        if (other.clock.isEmpty())
            return Order.AFTER;

        boolean anyLess = false;
        boolean anyGreater = false;
        for (String node : allNodes(clock, other.clock)) {
            long a = clock.getOrDefault(node, 0L);
            long b = other.clock.getOrDefault(node, 0L);
            if (a < b)
                anyLess = true;
            if (a > b)
                anyGreater = true;
        }
        if (anyLess && anyGreater)
            return Order.CONCURRENT;
        if (anyLess)
            return Order.BEFORE;
        if (anyGreater)
            return Order.AFTER;
        return Order.EQUAL;
    }

    private static Iterable<String> allNodes(Map<String, Long> a, Map<String, Long> b) {
        Map<String, Long> combined = new HashMap<>(a);
        combined.putAll(b);
        return combined.keySet();
    }

    /**
     * For read quorum: pick the "max" clock (happens-after or concurrent merged).
     */
    public VectorClock maxWith(VectorClock other) {
        Order order = compareTo(other);
        return switch (order) {
            case BEFORE, EQUAL -> other != null ? other : this;
            case AFTER, CONCURRENT -> order == Order.CONCURRENT ? merge(other) : this;
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        VectorClock that = (VectorClock) o;
        return clock.equals(that.clock);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clock);
    }

    @Override
    public String toString() {
        return "VectorClock" + new TreeMap<>(clock).toString();
    }
}
