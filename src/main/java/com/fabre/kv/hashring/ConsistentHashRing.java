package com.fabre.kv.hashring;

import com.fabre.kv.string.UrlNormalizer;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Consistent hash ring: nodes are identified by URL. Given a key, returns the R nodes responsible for that key
 * (successors on the ring). Supports adding and removing nodes at runtime (thread-safe).
 */
public class ConsistentHashRing {

    private static final int VIRTUAL_NODES_PER_NODE = 100;
    private final List<String> nodeUrls;
    private volatile TreeMap<Long, String> ring;

    public ConsistentHashRing(List<String> nodeUrls) {
        List<String> normalized = new ArrayList<>();
        if (nodeUrls != null) {
            for (String url : nodeUrls) {
                if (url != null && !url.isBlank()) {
                    normalized.add(UrlNormalizer.normalize(url));
                }
            }
        }
        this.nodeUrls = new CopyOnWriteArrayList<>(normalized);
        this.ring = buildRing(this.nodeUrls);
    }

    private static TreeMap<Long, String> buildRing(List<String> urls) {
        TreeMap<Long, String> r = new TreeMap<>();
        for (String url : urls) {
            for (int i = 0; i < VIRTUAL_NODES_PER_NODE; i++) {
                long hash = hash(url + "#" + i);
                r.put(hash, url);
            }
        }
        return r;
    }

    public synchronized void addNode(String url) {
        if (url == null || url.isBlank())
            return;
        String normalized = UrlNormalizer.normalize(url);
        if (nodeUrls.contains(normalized))
            return;
        nodeUrls.add(normalized);
        ring = buildRing(nodeUrls);
    }

    public synchronized void removeNode(String url) {
        if (url == null || url.isBlank())
            return;
        String normalized = UrlNormalizer.normalize(url);
        nodeUrls.remove(normalized);
        ring = buildRing(nodeUrls);
    }

    /**
     * Returns the R nodes responsible for the given key, in ring order.
     */
    public List<String> getNodesFor(String key, int r) {
        List<String> urls = this.nodeUrls;
        TreeMap<Long, String> rng = this.ring;
        if (urls.isEmpty())
            return List.of();
        if (r >= urls.size())
            return new ArrayList<>(urls);

        long keyHash = hash(key);
        List<String> result = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        Map<Long, String> tail = rng.tailMap(keyHash, false);
        for (String url : tail.values()) {
            if (seen.add(url)) {
                result.add(url);
                if (result.size() == r)
                    return result;
            }
        }
        for (String url : rng.values()) {
            if (seen.add(url)) {
                result.add(url);
                if (result.size() == r)
                    return result;
            }
        }
        return result;
    }

    /**
     * Returns an unmodifiable view of the current node list (no copy).
     */
    public List<String> getAllNodes() {
        return Collections.unmodifiableList(nodeUrls);
    }

    /** Number of nodes in the ring (O(1)). Use instead of getAllNodes().size() when only size is needed. */
    public int getNodeCount() {
        return nodeUrls.size();
    }

    private static long hash(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(s.getBytes(StandardCharsets.UTF_8));
            long h = 0;
            for (int i = 0; i < 8; i++) {
                h = (h << 8) | (digest[i] & 0xff);
            }
            return h;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
