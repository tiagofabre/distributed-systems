package com.fabre.kv.membership;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("MembershipView")
class MembershipViewTest {

    private static final List<String> INITIAL = List.of("http://n1", "http://n2", "http://n3");
    private static final String SELF = "http://n1";

    @Nested
    @DisplayName("addNode / removeNode")
    class AddRemoveNode {

        @Test
        void addNodeAddsToKnownAndAlive() {
            var view = new MembershipView(INITIAL, SELF);
            view.addNode("http://n4");
            assertTrue(view.getKnownNodes().contains("http://n4"));
            assertTrue(view.getAliveUrls().contains("http://n4"));
        }

        @Test
        void removeNodeRemovesFromKnownAndAlive() {
            var view = new MembershipView(INITIAL, SELF);
            view.removeNode("http://n2");
            assertFalse(view.getKnownNodes().contains("http://n2"));
            assertFalse(view.getAliveUrls().contains("http://n2"));
        }

        @Test
        void addRemoveIgnoresNullOrBlank() {
            var view = new MembershipView(INITIAL, SELF);
            int knownBefore = view.getKnownNodes().size();
            view.addNode(null);
            view.addNode("  ");
            view.removeNode(null);
            view.removeNode("");
            assertEquals(knownBefore, view.getKnownNodes().size());
        }
    }

    @Nested
    @DisplayName("getReplicasFor")
    class GetReplicasFor {

        @Test
        void returnsOnlyAliveReplicas() {
            var view = new MembershipView(INITIAL, SELF);
            view.markDead("http://n2");
            List<String> replicas = view.getReplicasFor("key1", 3);
            assertFalse(replicas.contains("http://n2"));
            assertTrue(replicas.size() <= 2);
        }

        @Test
        void returnsUpToRReplicas() {
            var view = new MembershipView(INITIAL, SELF);
            List<String> replicas = view.getReplicasFor("key1", 2);
            assertEquals(2, replicas.size());
        }

        @Test
        void allReturnedReplicasAreAlive() {
            var view = new MembershipView(INITIAL, SELF);
            List<String> replicas = view.getReplicasFor("key1", 3);
            for (String url : replicas) {
                assertTrue(view.isAlive(url));
            }
        }
    }

    @Nested
    @DisplayName("markAlive / markDead")
    class MarkAliveDead {

        @Test
        void markDeadRemovesFromAliveSet() {
            var view = new MembershipView(INITIAL, SELF);
            assertTrue(view.isAlive("http://n2"));
            view.markDead("http://n2");
            assertFalse(view.isAlive("http://n2"));
            assertTrue(view.getKnownNodes().contains("http://n2"));
        }

        @Test
        void markAliveAddsBackToAliveSet() {
            var view = new MembershipView(INITIAL, SELF);
            view.markDead("http://n2");
            view.markAlive("http://n2");
            assertTrue(view.isAlive("http://n2"));
        }
    }

    @Test
    @DisplayName("getSelfUrl returns normalized self")
    void getSelfUrlReturnsNormalized() {
        var view = new MembershipView(INITIAL, "http://n1/");
        assertEquals("http://n1", view.getSelfUrl());
    }

    @Test
    @DisplayName("getAliveUrls returns copy of alive set")
    void getAliveUrlsReturnsCopy() {
        var view = new MembershipView(INITIAL, SELF);
        Set<String> alive = view.getAliveUrls();
        assertEquals(3, alive.size());
        assertTrue(alive.contains("http://n1") && alive.contains("http://n2") && alive.contains("http://n3"));
    }

    @Nested
    @DisplayName("isReplicaFor / replica identity (regression)")
    class ReplicaIdentityRegression {

        @Test
        void whenReplicationFactorEqualsClusterSize_everyNodeIsReplicaForEveryKey() {
            List<String> nodes = List.of("http://127.0.0.1:8201", "http://127.0.0.1:8202", "http://127.0.0.1:8203");
            for (String selfUrl : nodes) {
                var view = new MembershipView(nodes, selfUrl);
                assertTrue(view.isReplicaFor("demo-key", 3),
                        "Node " + selfUrl + " should be replica for demo-key when r=3 and cluster size=3");
            }
        }

        @Test
        void getReplicasForReturnsNormalizedUrlsIncludingSelf() {
            List<String> nodes = List.of("http://127.0.0.1:8201", "http://127.0.0.1:8202", "http://127.0.0.1:8203");
            String selfUrl = "http://127.0.0.1:8201";
            var view = new MembershipView(nodes, selfUrl);
            List<String> replicas = view.getReplicasFor("demo-key", 3);
            assertTrue(replicas.contains("http://127.0.0.1:8201"),
                    "Replica list should contain normalized self URL: " + replicas);
        }

        @Test
        void isReplicaForTrueWhenInitialNodesHaveTrailingSlash() {
            List<String> nodesWithSlash = List.of("http://n1/", "http://n2/", "http://n3/");
            var view = new MembershipView(nodesWithSlash, "http://n1");
            assertTrue(view.isReplicaFor("any-key", 3));
        }

        @Test
        void isReplicaForTrueWhenSelfHasTrailingSlash() {
            var view = new MembershipView(INITIAL, "http://n1/");
            assertTrue(view.isReplicaFor("any-key", 3));
        }

        @Test
        @DisplayName("getReplicasFor returns URLs with scheme when ring has scheme-less URL (regression)")
        void getReplicasForReturnsUrlsWithSchemeWhenInitialNodesHaveSchemeLessUrl() {
            List<String> nodesWithMalformed = List.of("http://127.0.0.1:8201", "http://127.0.0.1:8202", "://127.0.0.1:8203");
            var view = new MembershipView(nodesWithMalformed, "http://127.0.0.1:8201");
            List<String> replicas = view.getReplicasFor("demo-key", 3);
            assertEquals(3, replicas.size());
            for (String url : replicas) {
                assertTrue(url.startsWith("http://") || url.startsWith("https://"),
                        "Replica URL must have scheme to be usable by HTTP client: " + url);
            }
        }
    }
}
