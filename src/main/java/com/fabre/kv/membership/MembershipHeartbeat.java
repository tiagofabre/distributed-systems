package com.fabre.kv.membership;

import com.fabre.kv.config.NodeConfig;
import com.fabre.kv.string.UrlNormalizer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Periodically pings known nodes (GET /health). Marks nodes alive or dead in MembershipView after N consecutive
 * successes or failures.
 */
public class MembershipHeartbeat {

    private final MembershipView membership;
    private final HttpClient httpClient;
    private final int intervalSeconds;
    private final int timeoutSeconds;
    private final int failuresBeforeDead;
    private final String selfUrl;
    private final Map<String, Integer> consecutiveFailures = new ConcurrentHashMap<>();
    private volatile ScheduledExecutorService scheduler;
    private volatile ScheduledFuture<?> task;

    public MembershipHeartbeat(MembershipView membership, NodeConfig config) {
        this.membership = membership;
        this.selfUrl = UrlNormalizer.normalize(config.getNodeUrl());
        this.intervalSeconds = config.getHeartbeatIntervalSeconds();
        this.timeoutSeconds = config.getHeartbeatTimeoutSeconds();
        this.failuresBeforeDead = config.getHeartbeatFailuresBeforeDead();
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(timeoutSeconds)).build();
    }

    public void start() {
        if (scheduler != null)
            return;
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "membership-heartbeat");
            t.setDaemon(true);
            return t;
        });
        task = scheduler.scheduleAtFixedRate(this::runRound, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }

    public void stop() {
        if (task != null) {
            task.cancel(false);
            task = null;
        }
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                scheduler.shutdownNow();
            }
            scheduler = null;
        }
    }

    private void runRound() {
        List<String> urlsToPing = new ArrayList<>();
        for (String url : membership.getKnownNodes()) {
            String norm = UrlNormalizer.normalize(url);
            if (norm.equals(selfUrl))
                continue;
            urlsToPing.add(url);
        }
        if (urlsToPing.isEmpty())
            return;

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (String url : urlsToPing) {
            final String targetUrl = url;
            final String norm = UrlNormalizer.normalize(url);
            String healthUrl = norm + "/health";
            HttpRequest req = HttpRequest.newBuilder().uri(URI.create(healthUrl))
                    .timeout(Duration.ofSeconds(timeoutSeconds)).GET().build();
            CompletableFuture<Void> future = httpClient.sendAsync(req,
                            java.net.http.HttpResponse.BodyHandlers.ofString())
                    .thenAccept(resp -> {
                        if (resp.statusCode() >= 200 && resp.statusCode() < 300) {
                            consecutiveFailures.remove(norm);
                            membership.markAlive(targetUrl);
                        } else {
                            recordFailure(norm, targetUrl);
                        }
                    }).exceptionally(e -> {
                        recordFailure(norm, targetUrl);
                        return null;
                    });
            futures.add(future);
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
    }

    private void recordFailure(String normUrl, String originalUrl) {
        int failures = consecutiveFailures.merge(normUrl, 1, Integer::sum);
        if (failures >= failuresBeforeDead) {
            membership.markDead(originalUrl);
        }
    }
}
