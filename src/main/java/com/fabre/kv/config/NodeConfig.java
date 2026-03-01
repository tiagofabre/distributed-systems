package com.fabre.kv.config;

import com.fabre.kv.string.UrlNormalizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.IntStream;

/**
 * Configuration for a KV node. Load from env and/or application.properties.
 * Precedence: env vars override defaults.
 */
public class NodeConfig {

    private static final Logger log = LoggerFactory.getLogger(NodeConfig.class);

    private final int port;
    private final String nodeUrl;
    private final List<String> nodes;
    private final int replicationFactor;
    private final int writeQuorum;
    private final int readQuorum;
    private final int requestTimeoutSeconds;
    private final int replicationTimeoutSeconds;
    private final int readReplicationTimeoutSeconds;
    private final int heartbeatIntervalSeconds;
    private final int heartbeatTimeoutSeconds;
    private final int heartbeatFailuresBeforeDead;
    private final String executorType;
    private final int executorCoreSize;
    private final int executorMaxSize;
    private final int executorQueueCapacity;
    private final int executorKeepAliveSeconds;
    private final String executorRejectedPolicy;
    private final int backpressureMaxConcurrentRequests;

    private NodeConfig(Builder b) {
        this.port = b.port;
        this.nodeUrl = b.nodeUrl;
        this.nodes = List.copyOf(b.nodes);
        this.replicationFactor = b.replicationFactor;
        this.writeQuorum = b.writeQuorum;
        this.readQuorum = b.readQuorum;
        this.requestTimeoutSeconds = b.requestTimeoutSeconds;
        this.replicationTimeoutSeconds = b.replicationTimeoutSeconds;
        this.readReplicationTimeoutSeconds = b.readReplicationTimeoutSeconds;
        this.heartbeatIntervalSeconds = b.heartbeatIntervalSeconds;
        this.heartbeatTimeoutSeconds = b.heartbeatTimeoutSeconds;
        this.heartbeatFailuresBeforeDead = b.heartbeatFailuresBeforeDead;
        this.executorType = b.executorType;
        this.executorCoreSize = b.executorCoreSize;
        this.executorMaxSize = b.executorMaxSize;
        this.executorQueueCapacity = b.executorQueueCapacity;
        this.executorKeepAliveSeconds = b.executorKeepAliveSeconds;
        this.executorRejectedPolicy = b.executorRejectedPolicy;
        this.backpressureMaxConcurrentRequests = b.backpressureMaxConcurrentRequests;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Default list of 15 node URLs for simulation: http://127.0.0.1:8201 .. 8215.
     */
    private static List<String> defaultFifteenNodes() {
        return IntStream.rangeClosed(8201, 8215).mapToObj(p -> "http://127.0.0.1:" + p).toList();
    }

    public int getPort() {
        return port;
    }

    public String getNodeUrl() {
        return nodeUrl;
    }

    public List<String> getNodes() {
        return nodes;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public int getWriteQuorum() {
        return writeQuorum;
    }

    public int getReadQuorum() {
        return readQuorum;
    }

    public int getRequestTimeoutSeconds() {
        return requestTimeoutSeconds;
    }

    public int getReplicationTimeoutSeconds() {
        return replicationTimeoutSeconds;
    }

    public int getReadReplicationTimeoutSeconds() {
        return readReplicationTimeoutSeconds;
    }

    public int getHeartbeatIntervalSeconds() {
        return heartbeatIntervalSeconds;
    }

    public int getHeartbeatTimeoutSeconds() {
        return heartbeatTimeoutSeconds;
    }

    public int getHeartbeatFailuresBeforeDead() {
        return heartbeatFailuresBeforeDead;
    }

    public String getExecutorType() {
        return executorType;
    }

    public int getExecutorCoreSize() {
        return executorCoreSize;
    }

    public int getExecutorMaxSize() {
        return executorMaxSize;
    }

    public int getExecutorQueueCapacity() {
        return executorQueueCapacity;
    }

    public int getExecutorKeepAliveSeconds() {
        return executorKeepAliveSeconds;
    }

    public String getExecutorRejectedPolicy() {
        return executorRejectedPolicy;
    }

    public int getBackpressureMaxConcurrentRequests() {
        return backpressureMaxConcurrentRequests;
    }

    public static final class Builder {
        private int port = 8001;
        private String nodeUrl = "http://127.0.0.1:8201";
        private List<String> nodes = defaultFifteenNodes();
        private int replicationFactor = 15;
        private int writeQuorum = 2;
        private int readQuorum = 2;
        private int requestTimeoutSeconds = 10;
        private int replicationTimeoutSeconds = 3;
        private int readReplicationTimeoutSeconds = 1;
        private int heartbeatIntervalSeconds = 2;
        private int heartbeatTimeoutSeconds = 1;
        private int heartbeatFailuresBeforeDead = 3;
        private String executorType = "fixed_pool";
        private int executorCoreSize = 4;
        private int executorMaxSize = 32;
        private int executorQueueCapacity = 256;
        private int executorKeepAliveSeconds = 60;
        private String executorRejectedPolicy = "abort";
        private int backpressureMaxConcurrentRequests = 500;

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder nodeUrl(String nodeUrl) {
            this.nodeUrl = nodeUrl;
            return this;
        }

        public Builder nodes(List<String> nodes) {
            this.nodes = nodes;
            return this;
        }

        public Builder replicationFactor(int n) {
            this.replicationFactor = n;
            return this;
        }

        public Builder writeQuorum(int n) {
            this.writeQuorum = n;
            return this;
        }

        public Builder readQuorum(int n) {
            this.readQuorum = n;
            return this;
        }

        public Builder requestTimeoutSeconds(int n) {
            this.requestTimeoutSeconds = n;
            return this;
        }

        public Builder replicationTimeoutSeconds(int n) {
            this.replicationTimeoutSeconds = n;
            return this;
        }

        public Builder readReplicationTimeoutSeconds(int n) {
            this.readReplicationTimeoutSeconds = n;
            return this;
        }

        public Builder heartbeatIntervalSeconds(int n) {
            this.heartbeatIntervalSeconds = n;
            return this;
        }

        public Builder heartbeatTimeoutSeconds(int n) {
            this.heartbeatTimeoutSeconds = n;
            return this;
        }

        public Builder heartbeatFailuresBeforeDead(int n) {
            this.heartbeatFailuresBeforeDead = n;
            return this;
        }

        public Builder executorType(String s) {
            this.executorType = s;
            return this;
        }

        public Builder executorCoreSize(int n) {
            this.executorCoreSize = n;
            return this;
        }

        public Builder executorMaxSize(int n) {
            this.executorMaxSize = n;
            return this;
        }

        public Builder executorQueueCapacity(int n) {
            this.executorQueueCapacity = n;
            return this;
        }

        public Builder executorKeepAliveSeconds(int n) {
            this.executorKeepAliveSeconds = n;
            return this;
        }

        public Builder executorRejectedPolicy(String s) {
            this.executorRejectedPolicy = s;
            return this;
        }

        public Builder backpressureMaxConcurrentRequests(int n) {
            this.backpressureMaxConcurrentRequests = n;
            return this;
        }

        public NodeConfig build() {
            if (replicationFactor <= 0) {
                throw new IllegalArgumentException(
                        "replicationFactor must be positive, got " + replicationFactor);
            }
            if (writeQuorum < 1 || writeQuorum > replicationFactor) {
                throw new IllegalArgumentException(
                        "writeQuorum must be between 1 and replicationFactor ("
                                + replicationFactor + "), got " + writeQuorum);
            }
            if (readQuorum < 1 || readQuorum > replicationFactor) {
                throw new IllegalArgumentException(
                        "readQuorum must be between 1 and replicationFactor ("
                                + replicationFactor + "), got " + readQuorum);
            }
            return new NodeConfig(this);
        }
    }

    @FunctionalInterface
    private interface IntSetter {
        void set(int v);
    }

    @FunctionalInterface
    private interface StrSetter {
        void set(String s);
    }

    private static void setIntEnv(String name, IntSetter setter) {
        String v = System.getenv(name);
        if (v == null || v.isBlank())
            return;
        try {
            setter.set(Integer.parseInt(v.trim()));
        } catch (NumberFormatException e) {
            log.warn("Invalid env {}={}, using default", name, v, e);
        }
    }

    private static void setStrEnv(String name, StrSetter setter) {
        String v = System.getenv(name);
        if (v != null && !v.isBlank())
            setter.set(v.trim());
    }

    public static NodeConfig fromEnv() {
        Builder b = builder();
        String portStr = System.getenv("PORT");
        if (portStr != null && !portStr.isBlank()) {
            try {
                b.port(Integer.parseInt(portStr.trim()));
            } catch (NumberFormatException e) {
                log.warn("Invalid env PORT={}, using default", portStr, e);
            }
        }
        String nodeUrl = System.getenv("NODE_URL");
        if (nodeUrl != null && !nodeUrl.isBlank())
            b.nodeUrl(nodeUrl.trim());
        String nodesStr = System.getenv("NODES");
        if (nodesStr != null && !nodesStr.isBlank()) {
            b.nodes(parseNodesFromString(nodesStr));
        }
        setIntEnv("REPLICATION_FACTOR", b::replicationFactor);
        setIntEnv("WRITE_QUORUM", b::writeQuorum);
        setIntEnv("READ_QUORUM", b::readQuorum);
        setIntEnv("REQUEST_TIMEOUT_SECONDS", b::requestTimeoutSeconds);
        setIntEnv("REPLICATION_TIMEOUT_SECONDS", b::replicationTimeoutSeconds);
        setIntEnv("READ_REPLICATION_TIMEOUT_SECONDS", b::readReplicationTimeoutSeconds);
        setIntEnv("HEARTBEAT_INTERVAL_SECONDS", b::heartbeatIntervalSeconds);
        setIntEnv("HEARTBEAT_TIMEOUT_SECONDS", b::heartbeatTimeoutSeconds);
        setIntEnv("HEARTBEAT_FAILURES_BEFORE_DEAD", b::heartbeatFailuresBeforeDead);
        setStrEnv("EXECUTOR_TYPE", b::executorType);
        setIntEnv("EXECUTOR_CORE_SIZE", b::executorCoreSize);
        setIntEnv("EXECUTOR_MAX_SIZE", b::executorMaxSize);
        setIntEnv("EXECUTOR_QUEUE_CAPACITY", b::executorQueueCapacity);
        setIntEnv("EXECUTOR_KEEP_ALIVE_SECONDS", b::executorKeepAliveSeconds);
        setStrEnv("EXECUTOR_REJECTED_POLICY", b::executorRejectedPolicy);
        setIntEnv("BACKPRESSURE_MAX_CONCURRENT_REQUESTS", b::backpressureMaxConcurrentRequests);
        return b.build();
    }

    /**
     * Parses a comma-separated NODES string and returns a list of URLs with scheme ensured. Used by fromEnv(); exposed
     * package-private for tests.
     */
    static List<String> parseNodesFromString(String nodesStr) {
        if (nodesStr == null || nodesStr.isBlank())
            return List.of();
        return java.util.Arrays.stream(nodesStr.trim().split("\\s*,\\s*")).map(String::trim)
                .filter(s -> !s.isEmpty()).map(UrlNormalizer::ensureScheme).toList();
    }
}
