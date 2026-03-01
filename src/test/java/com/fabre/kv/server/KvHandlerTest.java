package com.fabre.kv.server;

import com.fabre.kv.config.NodeConfig;
import com.fabre.kv.membership.MembershipView;
import com.fabre.kv.replication.ReplicationClient;
import com.fabre.kv.store.InMemoryStore;
import com.fabre.kv.store.StoredValue;
import com.fabre.kv.store.VectorClock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KvHandlerTest {

    private static final class TrackingReplicationClient extends ReplicationClient {
        private int getCalls;
        private int putToReplicasCalls;
        private boolean putToReplicasReturns = true;

        private TrackingReplicationClient(NodeConfig config) {
            super(config);
        }

        void setPutToReplicasReturns(boolean value) {
            this.putToReplicasReturns = value;
        }

        @Override
        public List<StoredValue> getFromReplicas(String key, List<String> replicaUrls, String selfUrl, int maxResults) {
            getCalls++;
            return new ArrayList<>();
        }

        @Override
        public boolean putToReplicas(String key, String value, VectorClock clock, List<String> replicaUrls,
                String selfUrl) {
            putToReplicasCalls++;
            return putToReplicasReturns;
        }
    }

    @Test
    void internalReadDoesNotFanOutToReplicas() {
        String selfUrl = "http://127.0.0.1:8201";
        NodeConfig config = NodeConfig.builder().nodeUrl(selfUrl).nodes(List.of(selfUrl)).replicationFactor(1)
                .readQuorum(1).writeQuorum(1).build();
        InMemoryStore store = new InMemoryStore();
        store.put("k", "v", new VectorClock());
        MembershipView membership = new MembershipView(config.getNodes(), selfUrl);
        TrackingReplicationClient replicationClient = new TrackingReplicationClient(config);
        KvHandler handler = new KvHandler(store, config, membership, replicationClient);

        HttpResponse response = handler.handleGet("k", true);

        assertEquals(200, response.statusCode());
        assertEquals(0, replicationClient.getCalls);
    }

    @Nested
    @DisplayName("handlePut with clock and internalReplication")
    class PutWithClock {

        @Test
        @DisplayName("internalReplication true: only store.put, no putToReplicas, returns 204")
        void internalReplicationTrueNoFanOut() throws Exception {
            String selfUrl = "http://127.0.0.1:8201";
            NodeConfig config = NodeConfig.builder().nodeUrl(selfUrl).nodes(List.of(selfUrl)).replicationFactor(1)
                    .readQuorum(1).writeQuorum(1).build();
            InMemoryStore store = new InMemoryStore();
            MembershipView membership = new MembershipView(config.getNodes(), selfUrl);
            TrackingReplicationClient replicationClient = new TrackingReplicationClient(config);
            KvHandler handler = new KvHandler(store, config, membership, replicationClient);

            String body = "{\"value\":\"x\",\"clock\":{\"http://n1\":1}}";
            HttpResponse response = handler.handlePut("key", body.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    true);

            assertEquals(204, response.statusCode());
            assertEquals(0, replicationClient.putToReplicasCalls);
            assertTrue(store.get("key").isPresent());
            assertEquals("x", store.get("key").get().value());
        }

        @Test
        @DisplayName("internalReplication false and clock: calls putToReplicas, returns 204 when quorum")
        void internalReplicationFalseCallsPutToReplicas204() throws Exception {
            String selfUrl = "http://127.0.0.1:8201";
            NodeConfig config = NodeConfig.builder().nodeUrl(selfUrl).nodes(List.of(selfUrl)).replicationFactor(1)
                    .readQuorum(1).writeQuorum(1).build();
            InMemoryStore store = new InMemoryStore();
            MembershipView membership = new MembershipView(config.getNodes(), selfUrl);
            TrackingReplicationClient replicationClient = new TrackingReplicationClient(config);
            replicationClient.setPutToReplicasReturns(true);
            KvHandler handler = new KvHandler(store, config, membership, replicationClient);

            String body = "{\"value\":\"y\",\"clock\":{\"http://n1\":2}}";
            HttpResponse response = handler.handlePut("k", body.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    false);

            assertEquals(204, response.statusCode());
            assertEquals(1, replicationClient.putToReplicasCalls);
        }

        @Test
        @DisplayName("internalReplication false and clock: returns 503 when putToReplicas returns false")
        void internalReplicationFalse503WhenNoQuorum() throws Exception {
            String selfUrl = "http://127.0.0.1:8201";
            NodeConfig config = NodeConfig.builder().nodeUrl(selfUrl).nodes(List.of(selfUrl)).replicationFactor(1)
                    .readQuorum(1).writeQuorum(1).build();
            InMemoryStore store = new InMemoryStore();
            MembershipView membership = new MembershipView(config.getNodes(), selfUrl);
            TrackingReplicationClient replicationClient = new TrackingReplicationClient(config);
            replicationClient.setPutToReplicasReturns(false);
            KvHandler handler = new KvHandler(store, config, membership, replicationClient);

            String body = "{\"value\":\"z\",\"clock\":{\"http://n1\":3}}";
            HttpResponse response = handler.handlePut("k", body.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    false);

            assertEquals(503, response.statusCode());
            assertEquals(1, replicationClient.putToReplicasCalls);
        }
    }

    @Nested
    @DisplayName("handleGet merge clocks")
    class GetMergeClocks {

        @Test
        @DisplayName("returns value with AFTER clock and merged clock in response")
        void mergePicksAfterValueAndMergedClock() {
            String selfUrl = "http://127.0.0.1:8201";
            List<String> nodes = List.of("http://127.0.0.1:8201", "http://127.0.0.1:8202");
            NodeConfig config = NodeConfig.builder().nodeUrl(selfUrl).nodes(nodes).replicationFactor(2)
                    .readQuorum(2).writeQuorum(1).build();
            InMemoryStore store = new InMemoryStore();
            VectorClock clock1 = new VectorClock(Map.of("http://n1", 1L));
            store.put("key", "v1", clock1);
            MembershipView membership = new MembershipView(nodes, selfUrl);
            ReplicationClient replicationClient = new ReplicationClient(config) {
                @Override
                public List<StoredValue> getFromReplicas(String key, List<String> replicaUrls, String selfUrl,
                        int maxResults) {
                    VectorClock clock2 = new VectorClock(Map.of("http://n1", 1L, "http://n2", 1L));
                    return List.of(new StoredValue("v2", clock2));
                }
            };
            KvHandler handler = new KvHandler(store, config, membership, replicationClient);

            HttpResponse response = handler.handleGet("key", false);

            assertEquals(200, response.statusCode());
            String json = new String(response.body() != null ? response.body() : new byte[0],
                    java.nio.charset.StandardCharsets.UTF_8);
            assertTrue(json.contains("\"value\":\"v2\""));
            assertTrue(json.contains("\"clock\""));
        }
    }

    @Test
    @DisplayName("handlePut empty key returns 400")
    void putEmptyKeyReturns400() {
        NodeConfig config = NodeConfig.builder().nodeUrl("http://n1").nodes(List.of("http://n1")).replicationFactor(1)
                .readQuorum(1).writeQuorum(1).build();
        KvHandler handler = new KvHandler(new InMemoryStore(), config, new MembershipView(List.of("http://n1"), "http://n1"),
                new ReplicationClient(config));
        HttpResponse response = handler.handlePut("", "{\"value\":\"x\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertEquals(400, response.statusCode());
    }

    @Test
    @DisplayName("handleGet empty key returns 400")
    void getEmptyKeyReturns400() {
        NodeConfig config = NodeConfig.builder().nodeUrl("http://n1").nodes(List.of("http://n1")).replicationFactor(1)
                .readQuorum(1).writeQuorum(1).build();
        KvHandler handler = new KvHandler(new InMemoryStore(), config, new MembershipView(List.of("http://n1"), "http://n1"),
                new ReplicationClient(config));
        HttpResponse response = handler.handleGet("", false);
        assertEquals(400, response.statusCode());
    }
}
