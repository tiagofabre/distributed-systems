package com.fabre.kv.replication;

import com.fabre.kv.store.StoredValue;
import com.fabre.kv.store.VectorClock;
import com.fabre.kv.string.UrlNormalizer;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Sends PUT/GET to replica nodes and aggregates responses for quorum. After write quorum is reached, continues to push
 * the write to replicas that did not ack (push-after-quorum) so they eventually converge.
 */
public class ReplicationClient {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String INTERNAL_READ_HEADER = "X-Internal-Read";
    private static final String INTERNAL_READ_VALUE = "1";
    private static final String INTERNAL_REPLICATION_HEADER = "X-Internal-Replication";

    /** Connect timeout is 2s; per-request timeout is from config (replication/read timeout). */
    private final HttpClient httpClient;
    private final int writeQuorum;
    private final com.fabre.kv.config.NodeConfig config;
    private final ExecutorService pushAfterQuorumExecutor;

    public ReplicationClient(com.fabre.kv.config.NodeConfig config) {
        this.config = config;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .build();
        this.writeQuorum = config.getWriteQuorum();
        this.pushAfterQuorumExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "replication-push-after-quorum");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Sends PUT to the given replica URLs (excluding self). Returns true if at least W accepted. Requests are sent in
     * parallel so total wait is at most one timeout when replicas are slow. After quorum is reached, replicas that did
     * not ack are pushed in the background (push-after-quorum) so they eventually converge.
     *
     * @param selfIsReplica
     *            true if the caller already performed a local write (so quorum is success+1 >= W).
     */
    public boolean putToReplicas(String key, String value, VectorClock clock, List<String> replicaUrls, String selfUrl,
            boolean selfIsReplica) {
        String body = jsonPutBody(value, clock);
        String selfNorm = UrlNormalizer.normalize(selfUrl);
        List<String> others = replicaUrls.stream().filter(u -> !UrlNormalizer.normalize(u).equals(selfNorm)).toList();
        if (others.isEmpty())
            return selfIsReplica;

        List<ReplicaPut> puts = new ArrayList<>();
        for (String baseUrl : others) {
            String url = UrlNormalizer.ensureScheme(baseUrl) + "/kv/" + key;
            HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url))
                    .timeout(Duration.ofSeconds(config.getReplicationTimeoutSeconds()))
                    .header("Content-Type", "application/json")
                    .header(INTERNAL_REPLICATION_HEADER, INTERNAL_READ_VALUE)
                    .PUT(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)).build();
            CompletableFuture<Boolean> future = httpClient
                    .sendAsync(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
                    .thenApply(resp -> resp.statusCode() == 204).exceptionally(e -> false);
            puts.add(new ReplicaPut(baseUrl, future));
        }

        int success = 0;
        List<String> failed = new ArrayList<>();
        for (ReplicaPut p : puts) {
            boolean ack = Boolean.TRUE.equals(p.future.join());
            if (ack)
                success++;
            else
                failed.add(p.baseUrl);
        }

        boolean quorumReached = (selfIsReplica ? success + 1 : success) >= writeQuorum;
        if (quorumReached && !failed.isEmpty()) {
            pushToReplicasInBackground(key, value, clock, failed);
        }
        return quorumReached;
    }

    /**
     * PUT value+clock to the given replica URLs (excluding self). Returns true if at least W accepted. Requests are
     * sent in parallel so total wait is at most one timeout when replicas are slow. After quorum is reached, replicas
     * that did not ack are pushed in the background (push-after-quorum) so they eventually converge.
     */
    public boolean putToReplicas(String key, String value, VectorClock clock, List<String> replicaUrls,
            String selfUrl) {
        return putToReplicas(key, value, clock, replicaUrls, selfUrl, true);
    }

    /**
     * Forwards a PUT request (raw body) to replica nodes. Used when this node is not a replica: it proxies to replicas
     * until one accepts (204). Returns true if at least one replica accepted, false if all failed.
     */
    public boolean forwardPut(String key, byte[] bodyBytes, List<String> replicaUrls) {
        if (bodyBytes == null || bodyBytes.length == 0 || replicaUrls.isEmpty())
            return false;
        String body = new String(bodyBytes, StandardCharsets.UTF_8);
        for (String baseUrl : replicaUrls) {
            String url = UrlNormalizer.ensureScheme(baseUrl) + "/kv/" + key;
            HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url))
                    .timeout(Duration.ofSeconds(config.getReplicationTimeoutSeconds()))
                    .header("Content-Type", "application/json")
                    .header(INTERNAL_REPLICATION_HEADER, INTERNAL_READ_VALUE)
                    .PUT(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)).build();
            try {
                HttpResponse<String> resp = httpClient.send(req,
                        HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                if (resp.statusCode() == 204)
                    return true;
            } catch (Exception e) {
                // try next replica
            }
        }
        return false;
    }

    /**
     * Forwards a GET request to one replica. Used when this node is not a replica: proxies to replicas until one
     * returns 200 or 404. Uses X-Internal-Read so the replica responds from local store only. Returns empty if all
     * replicas failed or returned 503.
     */
    public Optional<ForwardGetResult> forwardGet(String key, List<String> replicaUrls) {
        if (replicaUrls.isEmpty())
            return Optional.empty();
        int timeoutSeconds = config.getReadReplicationTimeoutSeconds();
        Duration timeout = Duration.ofSeconds(timeoutSeconds);
        for (String baseUrl : replicaUrls) {
            String url = UrlNormalizer.ensureScheme(baseUrl) + "/kv/" + key;
            HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url)).timeout(timeout)
                    .header(INTERNAL_READ_HEADER, INTERNAL_READ_VALUE).GET().build();
            try {
                HttpResponse<String> resp = httpClient.send(req,
                        HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                int code = resp.statusCode();
                if (code == 200 || code == 404) {
                    byte[] body = resp.body() != null ? resp.body().getBytes(StandardCharsets.UTF_8) : new byte[0];
                    return Optional.of(new ForwardGetResult(code, body));
                }
            } catch (Exception e) {
                // try next replica
            }
        }
        return Optional.empty();
    }

    /** Result of a forwarded GET: status code and JSON body from the replica. */
    public record ForwardGetResult(int statusCode, byte[] body) {
    }

    /**
     * Sends PUT to the given replica URLs in the background (fire-and-forget). Used after quorum so remaining replicas
     * eventually receive the write.
     */
    private void pushToReplicasInBackground(String key, String value, VectorClock clock, List<String> replicaUrls) {
        String body = jsonPutBody(value, clock);
        pushAfterQuorumExecutor.execute(() -> {
            for (String baseUrl : replicaUrls) {
                String url = UrlNormalizer.ensureScheme(baseUrl) + "/kv/" + key;
                HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url))
                        .timeout(Duration.ofSeconds(config.getReplicationTimeoutSeconds()))
                        .header("Content-Type", "application/json")
                        .header(INTERNAL_REPLICATION_HEADER, INTERNAL_READ_VALUE)
                        .PUT(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)).build();
                httpClient.sendAsync(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
                        .thenAccept(resp -> {
                        }).exceptionally(e -> null);
            }
        });
    }

    private record ReplicaPut(String baseUrl, CompletableFuture<Boolean> future) {
    }

    /**
     * GET from the given replica URLs (excluding self). Returns list of successful (value, clock) responses. Requests
     * are sent in parallel so total wait is at most one timeout when replicas are down.
     */
    public List<StoredValue> getFromReplicas(String key, List<String> replicaUrls, String selfUrl) {
        return getFromReplicas(key, replicaUrls, selfUrl, Integer.MAX_VALUE);
    }

    /**
     * GET from replicas and stop collecting once maxResults successful responses are available. This allows the caller
     * to stop early as soon as read quorum is satisfied.
     */
    public List<StoredValue> getFromReplicas(String key, List<String> replicaUrls, String selfUrl, int maxResults) {
        String selfNorm = UrlNormalizer.normalize(selfUrl);
        List<String> others = replicaUrls.stream().filter(u -> !UrlNormalizer.normalize(u).equals(selfNorm)).toList();
        if (others.isEmpty() || maxResults <= 0)
            return List.of();

        List<CompletableFuture<Optional<StoredValue>>> futures = new ArrayList<>();
        for (String baseUrl : others) {
            String url = UrlNormalizer.ensureScheme(baseUrl) + "/kv/" + key;
            HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url))
                    .timeout(Duration.ofSeconds(config.getReadReplicationTimeoutSeconds()))
                    .header(INTERNAL_READ_HEADER, INTERNAL_READ_VALUE).GET().build();
            CompletableFuture<Optional<StoredValue>> future = httpClient
                    .sendAsync(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
                    .thenApply(resp -> resp.statusCode() == 200 ? parseGetResponse(resp.body())
                            : Optional.<StoredValue> empty())
                    .exceptionally(e -> Optional.empty());
            futures.add(future);
        }

        List<StoredValue> results = new ArrayList<>();
        List<CompletableFuture<Optional<StoredValue>>> pending = new ArrayList<>(futures);
        while (!pending.isEmpty() && results.size() < maxResults) {
            CompletableFuture.anyOf(pending.toArray(CompletableFuture[]::new)).join();
            List<CompletableFuture<Optional<StoredValue>>> done = pending.stream().filter(CompletableFuture::isDone)
                    .toList();
            for (CompletableFuture<Optional<StoredValue>> f : done) {
                f.join().ifPresent(v -> {
                    if (results.size() < maxResults) {
                        results.add(v);
                    }
                });
            }
            pending.removeAll(done);
        }

        for (CompletableFuture<Optional<StoredValue>> f : pending) {
            f.cancel(true);
        }
        return results;
    }

    /**
     * Builds PUT body JSON without ObjectNode/valueToTree to avoid extra allocations and Jackson overhead on the hot
     * path. Escapes value and serializes clock map.
     */
    private static String jsonPutBody(String value, VectorClock clock) {
        String escapedValue = escapeJsonString(value != null ? value : "");
        String clockJson = clockMapToJson(clock.getClock());
        return "{\"value\":" + escapedValue + ",\"clock\":" + clockJson + "}";
    }

    /**
     * Package-private for tests: returns the same JSON PUT body as used for replication. Allows tests to verify
     * that keys and values with special characters are escaped and produce valid JSON.
     */
    static String jsonPutBodyForTest(String value, VectorClock clock) {
        return jsonPutBody(value, clock);
    }

    private static String escapeJsonString(String s) {
        return "\"" + escapeJsonContent(s != null ? s : "") + "\"";
    }

    /**
     * Escapes a string for use inside JSON double quotes. Used for both value (with quotes added by caller) and
     * object keys (caller adds quotes in clockMapToJson).
     */
    private static String escapeJsonContent(String s) {
        if (s == null)
            return "";
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < ' ')
                        sb.append(String.format("\\u%04x", (int) c));
                    else
                        sb.append(c);
                }
            }
        }
        return sb.toString();
    }

    private static String clockMapToJson(Map<String, Long> clock) {
        if (clock == null || clock.isEmpty())
            return "{}";
        StringBuilder sb = new StringBuilder().append('{');
        boolean first = true;
        for (Map.Entry<String, Long> e : clock.entrySet()) {
            if (!first)
                sb.append(',');
            first = false;
            sb.append('"').append(escapeJsonKey(e.getKey())).append("\":").append(e.getValue());
        }
        return sb.append('}').toString();
    }

    private static String escapeJsonKey(String s) {
        return escapeJsonContent(s);
    }

    private static Optional<StoredValue> parseGetResponse(String body) {
        try {
            var node = JSON.readTree(body);
            if (!node.has("value"))
                return Optional.empty();
            String value = node.get("value").asText();
            VectorClock clock = new VectorClock();
            if (node.has("clock") && node.get("clock").isObject()) {
                var clockNode = node.get("clock");
                java.util.Map<String, Long> map = new java.util.HashMap<>();
                clockNode.fields().forEachRemaining(e -> map.put(e.getKey(), e.getValue().asLong()));
                clock = new VectorClock(map);
            }
            return Optional.of(new StoredValue(value, clock));
        } catch (Exception e) {
            return Optional.empty();
        }
    }
}
