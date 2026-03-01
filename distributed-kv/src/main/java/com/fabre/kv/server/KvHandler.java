package com.fabre.kv.server;

import com.fabre.kv.config.NodeConfig;
import com.fabre.kv.membership.MembershipView;
import com.fabre.kv.replication.ReplicationClient;
import com.fabre.kv.store.InMemoryStore;
import com.fabre.kv.store.StoredValue;
import com.fabre.kv.store.VectorClock;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * KV and admin HTTP logic: server-agnostic. Returns HttpResponse for each action.
 */
public class KvHandler {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final InMemoryStore store;
    private final NodeConfig config;
    private final MembershipView membership;
    private final ReplicationClient replicationClient;

    public KvHandler(InMemoryStore store, NodeConfig config, MembershipView membership,
            ReplicationClient replicationClient) {
        this.store = store;
        this.config = config;
        this.membership = membership;
        this.replicationClient = replicationClient;
    }

    /** Replication count for replica selection: at most the number of known nodes. */
    private int effectiveReplication() {
        int n = membership.getKnownNodes().size();
        return n == 0 ? 0 : Math.min(config.getReplicationFactor(), n);
    }

    public HttpResponse handleHealth() {
        return HttpResponse.json(200, Map.of("status", "ok"));
    }

    public HttpResponse handleListNodes() {
        List<Map<String, Object>> nodes = new ArrayList<>();
        for (String url : membership.getKnownNodes()) {
            nodes.add(Map.of("url", url, "alive", membership.isAlive(url)));
        }
        return HttpResponse.json(200, nodes);
    }

    public HttpResponse handleAddNode(byte[] bodyBytes) {
        if (bodyBytes == null || bodyBytes.length == 0) {
            return HttpResponse.json(400, Map.of("error", "body required"));
        }
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = JSON.readValue(bodyBytes, Map.class);
            Object u = map.get("url");
            if (u == null || u.toString().isBlank()) {
                return HttpResponse.json(400, Map.of("error", "url required"));
            }
            membership.addNode(u.toString().trim());
            return HttpResponse.noContent();
        } catch (Exception e) {
            return HttpResponse.json(400, Map.of("error", "invalid json or url required"));
        }
    }

    public HttpResponse handleRemoveNode(String url) {
        if (url == null || url.isBlank()) {
            return HttpResponse.json(400, Map.of("error", "url query param required"));
        }
        membership.removeNode(url.trim());
        return HttpResponse.noContent();
    }

    public HttpResponse handleGet(String key) {
        return handleGet(key, false);
    }

    /**
     * When internalRead=true, respond from local store only (no fan-out) to avoid recursive replica reads between
     * nodes.
     */
    public HttpResponse handleGet(String key, boolean internalRead) {
        if (key == null || key.isBlank()) {
            return HttpResponse.json(400, Map.of("error", "empty key"));
        }

        int r = effectiveReplication();
        List<String> replicaUrls = membership.getReplicasFor(key, r);
        if (!membership.isReplicaFor(key, r)) {
            return replicationClient.forwardGet(key, replicaUrls)
                    .map(result -> new HttpResponse(result.statusCode(), "application/json",
                            result.body() != null ? result.body() : new byte[0]))
                    .orElse(HttpResponse.json(503, Map.of("error", "read quorum not reached")));
        }

        List<StoredValue> results = new ArrayList<>();
        store.get(key).ifPresent(results::add);
        if (!internalRead) {
            int remainingForQuorum = Math.max(0, config.getReadQuorum() - results.size());
            results.addAll(
                    replicationClient.getFromReplicas(key, replicaUrls, config.getNodeUrl(), remainingForQuorum));
        }

        if (internalRead) {
            if (results.isEmpty()) {
                return HttpResponse.json(404, Map.of("error", "not found"));
            }
        } else if (results.size() < config.getReadQuorum()) {
            if (results.isEmpty()) {
                return HttpResponse.json(503, Map.of("error", "read quorum not reached"));
            }
        }

        StoredValue merged = results.get(0);
        VectorClock mergedClock = merged.clock();
        for (int i = 1; i < results.size(); i++) {
            StoredValue next = results.get(i);
            mergedClock = mergedClock.merge(next.clock());
            if (merged.clock().compareTo(next.clock()) == VectorClock.Order.BEFORE) {
                merged = next;
            }
        }
        merged = new StoredValue(merged.value(), mergedClock);

        return HttpResponse.json(200, Map.of("value", merged.value(), "clock", merged.clock().getClock()));
    }

    public HttpResponse handlePut(String key, byte[] bodyBytes) {
        return handlePut(key, bodyBytes, false);
    }

    /**
     * Handles PUT for a key. When internalReplication is true (request from another replica), only applies locally and
     * returns 204. When internalReplication is false (client request), applies and replicates; if body contains clock,
     * replicates to reach quorum.
     */
    public HttpResponse handlePut(String key, byte[] bodyBytes, boolean internalReplication) {
        if (key == null || key.isBlank()) {
            return HttpResponse.json(400, Map.of("error", "empty key"));
        }
        if (bodyBytes == null || bodyBytes.length == 0) {
            return HttpResponse.json(400, Map.of("error", "body required"));
        }

        PutBody parsed = parsePutBodyFromBytes(bodyBytes);
        if (parsed == null) {
            return HttpResponse.json(400, Map.of("error", "invalid json: expected {\"value\": \"...\"}"));
        }

        int r = effectiveReplication();
        List<String> replicaUrls = membership.getReplicasFor(key, r);
        if (replicaUrls.isEmpty()) {
            return HttpResponse.json(503, Map.of("error", "no replicas available for the key"));
        }

        if (!membership.isReplicaFor(key, r)) {
            boolean ok = replicationClient.forwardPut(key, bodyBytes, replicaUrls);
            if (!ok) {
                return HttpResponse.json(503, Map.of("error", "write quorum not reached"));
            }
            return HttpResponse.noContent();
        }

        if (parsed.clock != null) {
            store.put(key, parsed.value, parsed.clock);
            if (internalReplication) {
                return HttpResponse.noContent();
            }
            boolean ok = replicationClient.putToReplicas(key, parsed.value, parsed.clock, replicaUrls,
                    config.getNodeUrl());
            if (!ok) {
                return HttpResponse.json(503, Map.of("error", "write quorum not reached"));
            }
            return HttpResponse.noContent();
        }

        VectorClock baseClock = store.get(key).map(StoredValue::clock).orElse(new VectorClock());
        VectorClock newClock = baseClock.increment(config.getNodeUrl());
        store.put(key, parsed.value, newClock);
        boolean ok = replicationClient.putToReplicas(key, parsed.value, newClock, replicaUrls, config.getNodeUrl());
        if (!ok) {
            return HttpResponse.json(503, Map.of("error", "write quorum not reached"));
        }
        return HttpResponse.noContent();
    }

    private record PutBody(String value, VectorClock clock) {
    }

    private static PutBody parsePutBodyFromBytes(byte[] body) {
        try (JsonParser p = JSON.getFactory().createParser(body)) {
            if (p.nextToken() != JsonToken.START_OBJECT)
                return null;
            String value = null;
            VectorClock clock = null;
            while (p.nextToken() != JsonToken.END_OBJECT) {
                String name = p.getCurrentName();
                if (name == null)
                    continue;
                p.nextToken();
                if ("value".equals(name)) {
                    if (p.currentToken() != JsonToken.VALUE_STRING)
                        return null;
                    value = p.getText();
                } else if ("clock".equals(name)) {
                    if (p.currentToken() == JsonToken.VALUE_NULL)
                        continue;
                    if (p.currentToken() != JsonToken.START_OBJECT)
                        return null;
                    Map<String, Long> clockMap = new HashMap<>();
                    while (p.nextToken() != JsonToken.END_OBJECT) {
                        String k = p.getCurrentName();
                        if (k == null)
                            continue;
                        p.nextToken();
                        if (p.currentToken() == JsonToken.VALUE_NUMBER_INT
                                || p.currentToken() == JsonToken.VALUE_NUMBER_FLOAT) {
                            clockMap.put(k, p.getValueAsLong());
                        }
                    }
                    clock = new VectorClock(clockMap);
                }
            }
            return value != null ? new PutBody(value, clock) : null;
        } catch (IOException e) {
            return null;
        }
    }
}
