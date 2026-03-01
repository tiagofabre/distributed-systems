package com.fabre.kv.membership;

import com.fabre.kv.hashring.ConsistentHashRing;
import com.fabre.kv.string.UrlNormalizer;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Dynamic membership view: known nodes, alive set, and consistent hash ring. Replicas for a key are taken from the
 * consistent hash ring and filtered to alive nodes only.
 */
public class MembershipView {

    private final ConsistentHashRing ring;
    private final Set<String> aliveUrls;
    private final ConcurrentHashMap<ReplicaCacheKey, List<String>> replicaCache;
    private final AtomicLong replicaCacheVersion;
    private final String selfUrl;

    public MembershipView(List<String> initialNodes, String selfUrl) {
        this.selfUrl = UrlNormalizer.normalize(selfUrl);
        this.ring = new ConsistentHashRing(initialNodes != null ? initialNodes : List.of());
        this.aliveUrls = ConcurrentHashMap.newKeySet();
        this.replicaCache = new ConcurrentHashMap<>();
        this.replicaCacheVersion = new AtomicLong(0);
        for (String url : ring.getAllNodes()) {
            aliveUrls.add(UrlNormalizer.normalize(url));
        }
    }

    /**
     * Returns true if this node (self) is among the replicas for the key.
     */
    public boolean isReplicaFor(String key, int r) {
        int nodeCount = ring.getNodeCount();
        if (nodeCount == 0)
            return false;
        if (r >= nodeCount) {
            String self = getSelfUrl();
            for (String url : ring.getAllNodes()) {
                if (UrlNormalizer.normalize(url).equals(self))
                    return true;
            }
            return false;
        }
        List<String> replicaUrls = getReplicasFor(key, r);
        String self = getSelfUrl();
        for (String url : replicaUrls) {
            if (UrlNormalizer.normalize(url).equals(self))
                return true;
        }
        return false;
    }

    /**
     * Returns up to r replica URLs for the key, in ring order, that are currently alive.
     */
    public List<String> getReplicasFor(String key, int r) {
        long version = replicaCacheVersion.get();
        ReplicaCacheKey cacheKey = new ReplicaCacheKey(key, r, version);
        List<String> cached = replicaCache.get(cacheKey);
        if (cached != null) {
            return cached;
        }

        int maxCandidates = Math.max(r * 2, ring.getNodeCount());
        List<String> candidates = ring.getNodesFor(key, maxCandidates);
        List<String> result = new ArrayList<>();
        for (String url : candidates) {
            if (result.size() >= r)
                break;
            String norm = UrlNormalizer.normalize(url);
            if (aliveUrls.contains(norm)) {
                result.add(UrlNormalizer.ensureScheme(url));
            }
        }
        List<String> frozen = List.copyOf(result);
        List<String> previous = replicaCache.putIfAbsent(cacheKey, frozen);
        return previous != null ? previous : frozen;
    }

    public void addNode(String url) {
        if (url == null || url.isBlank())
            return;
        ring.addNode(url);
        aliveUrls.add(UrlNormalizer.normalize(url));
        invalidateReplicaCache();
    }

    public void removeNode(String url) {
        if (url == null || url.isBlank())
            return;
        String norm = UrlNormalizer.normalize(url);
        ring.removeNode(url);
        aliveUrls.remove(norm);
        invalidateReplicaCache();
    }

    public void markAlive(String url) {
        if (url == null || url.isBlank())
            return;
        if (aliveUrls.add(UrlNormalizer.normalize(url))) {
            invalidateReplicaCache();
        }
    }

    public void markDead(String url) {
        if (url == null || url.isBlank())
            return;
        if (aliveUrls.remove(UrlNormalizer.normalize(url))) {
            invalidateReplicaCache();
        }
    }

    public boolean isAlive(String url) {
        return url != null && aliveUrls.contains(UrlNormalizer.normalize(url));
    }

    public List<String> getKnownNodes() {
        return ring.getAllNodes();
    }

    public Set<String> getAliveUrls() {
        return Set.copyOf(aliveUrls);
    }

    public String getSelfUrl() {
        return selfUrl;
    }

    private void invalidateReplicaCache() {
        replicaCacheVersion.incrementAndGet();
        replicaCache.clear();
    }

    private record ReplicaCacheKey(String key, int replicas, long version) {
    }
}
