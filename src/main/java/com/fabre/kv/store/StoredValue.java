package com.fabre.kv.store;

import java.util.Objects;

/**
 * Value stored in the KV store: string value and its vector clock.
 */
public record StoredValue(String value, VectorClock clock) {

    public StoredValue {
        Objects.requireNonNull(value, "value");
        Objects.requireNonNull(clock, "clock");
    }
}
