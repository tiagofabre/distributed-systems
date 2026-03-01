package com.fabre.kv;

import com.fabre.kv.config.NodeConfig;
import com.fabre.kv.membership.MembershipHeartbeat;
import com.fabre.kv.membership.MembershipView;
import com.fabre.kv.replication.ReplicationClient;
import com.fabre.kv.server.KvHandler;
import com.fabre.kv.server.Server;
import com.fabre.kv.store.InMemoryStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entrypoint for the KV node. Dynamic membership and heartbeats for failure detection.
 */
public class KvNodeApplication {

    /**
     * Holder for a running node: server and heartbeat. Call stop() to stop both.
     */
    public static final class NodeRunner {
        private static final Logger log = LoggerFactory.getLogger(NodeRunner.class);
        private final Server server;
        private final MembershipHeartbeat heartbeat;

        public NodeRunner(Server server, MembershipHeartbeat heartbeat) {
            this.server = server;
            this.heartbeat = heartbeat;
        }

        public Server server() {
            return server;
        }

        public void stop() {
            if (heartbeat != null)
                heartbeat.stop();
            if (server != null) {
                try {
                    server.stop();
                } catch (Exception e) {
                    log.warn("Error stopping server during shutdown", e);
                }
            }
        }
    }

    /**
     * Starts a node with the given config. Fail-fast startup: bind happens before heartbeat starts, so startup errors
     * (e.g. port already in use) are surfaced immediately and do not leave a half-alive process.
     */
    public static NodeRunner startWithConfig(NodeConfig config) {
        InMemoryStore store = new InMemoryStore();
        MembershipView membership = new MembershipView(config.getNodes(), config.getNodeUrl());
        ReplicationClient replicationClient = new ReplicationClient(config);
        KvHandler kvHandler = new KvHandler(store, config, membership, replicationClient);
        MembershipHeartbeat heartbeat = new MembershipHeartbeat(membership, config);

        Server server = new Server(config.getPort(), kvHandler);
        try {
            server.start();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while starting server", e);
        } catch (Exception e) {
            try {
                server.stop();
            } catch (Exception ignored) {
            }
            throw (e instanceof RuntimeException r) ? r : new RuntimeException(e);
        }

        heartbeat.start();
        return new NodeRunner(server, heartbeat);
    }

    public static void main(String[] args) {
        NodeConfig config = NodeConfig.fromEnv();
        NodeRunner runner = startWithConfig(config);
        System.out.println("KV node listening on port " + config.getPort() + " (nodeUrl=" + config.getNodeUrl() + ")");
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            runner.stop();
        }
    }
}
