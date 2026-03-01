package com.fabre.kv.store;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe in-memory store: key -> (value, vector clock).
 */
public class InMemoryStore {

    private final ConcurrentHashMap<String, StoredValue> map = new ConcurrentHashMap<>();

    public Optional<StoredValue> get(String key) {
        return Optional.ofNullable(map.get(key));
    }

    /**
     * Put value with the given vector clock. Replaces any existing value for the key.
     */
    public void put(String key, String value, VectorClock clock) {
        map.put(key, new StoredValue(value, clock));
    }

    /**
     * Put when this node is the writer: increment this node's entry in the clock and store.
     */
    public void put(String key, String value, VectorClock existingClock, String thisNodeUrl) {
        VectorClock newClock = existingClock == null ? new VectorClock() : existingClock;
        newClock = newClock.increment(thisNodeUrl);
        put(key, value, newClock);
    }
}
