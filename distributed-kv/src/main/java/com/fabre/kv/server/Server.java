package com.fabre.kv.server;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.eclipse.jetty.io.Content;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.PathMappingsHandler;
import org.eclipse.jetty.http.pathmap.PathSpec;
import org.eclipse.jetty.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HTTP server that binds a port and dispatches requests to a handler. Uses Jetty PathMappingsHandler for path-based
 * routing. Business logic runs off the I/O thread via an executor.
 */
public final class Server {

    private static final Logger log = LoggerFactory.getLogger(Server.class);
    private static final int MAX_CONTENT_LENGTH = 1024 * 1024; // 1 MiB
    private static final String PREFIX_KV = "/kv/";
    private static final String PATH_HEALTH = "/health";
    private static final String PATH_ADMIN_NODES = "/admin/nodes";
    private static final String INTERNAL_READ_HEADER = "X-Internal-Read";
    private static final String INTERNAL_REPLICATION_HEADER = "X-Internal-Replication";

    private final int port;
    private final KvHandler handler;
    private final ExecutorService requestExecutor;
    private org.eclipse.jetty.server.Server jettyServer;

    public Server(int port, KvHandler handler) {
        this.port = port;
        this.handler = handler;
        int threads = Math.max(4, Runtime.getRuntime().availableProcessors() * 2);
        this.requestExecutor = Executors.newFixedThreadPool(threads);
    }

    public void start() throws Exception {
        jettyServer = new org.eclipse.jetty.server.Server();
        ServerConnector connector = new ServerConnector(jettyServer);
        connector.setPort(port);
        jettyServer.addConnector(connector);

        PathMappingsHandler router = new PathMappingsHandler();
        router.addMapping(PathSpec.from(PATH_HEALTH), new DispatchHandler(this::routeHealth));
        router.addMapping(PathSpec.from(PATH_ADMIN_NODES), new DispatchHandler(this::routeAdminNodes));
        router.addMapping(PathSpec.from("/kv/*"), new DispatchHandler(this::routeKv));
        router.addMapping(PathSpec.from("/*"), new DispatchHandler((method, path, queryString, body,
                internalRead, internalReplication) -> HttpResponse.json(404, Map.of("error", "not found"))));

        jettyServer.setHandler(router);
        jettyServer.start();
    }

    public void stop() throws Exception {
        if (jettyServer != null) {
            jettyServer.stop();
            jettyServer = null;
        }
        requestExecutor.shutdown();
    }

    private HttpResponse routeHealth(String method, String path, String queryString, byte[] body,
            boolean internalRead, boolean internalReplication) {
        if (!"GET".equals(method)) {
            return HttpResponse.json(404, Map.of("error", "not found"));
        }
        return handler.handleHealth();
    }

    private HttpResponse routeAdminNodes(String method, String path, String queryString, byte[] body,
            boolean internalRead, boolean internalReplication) {
        if ("GET".equals(method)) {
            return handler.handleListNodes();
        }
        if ("POST".equals(method)) {
            return handler.handleAddNode(body);
        }
        if ("DELETE".equals(method)) {
            String url = queryParam(queryString, "url");
            return handler.handleRemoveNode(url);
        }
        return HttpResponse.json(404, Map.of("error", "not found"));
    }

    private HttpResponse routeKv(String method, String path, String queryString, byte[] body,
            boolean internalRead, boolean internalReplication) {
        if (!path.startsWith(PREFIX_KV) || path.length() <= PREFIX_KV.length()) {
            return HttpResponse.json(404, Map.of("error", "not found"));
        }
        String key = path.substring(PREFIX_KV.length());
        if ("GET".equals(method)) {
            return handler.handleGet(key, internalRead);
        }
        if ("PUT".equals(method)) {
            return handler.handlePut(key, body, internalReplication);
        }
        return HttpResponse.json(404, Map.of("error", "not found"));
    }

    private static String queryParam(String queryString, String name) {
        if (queryString == null || queryString.isEmpty()) {
            return null;
        }
        for (String pair : queryString.split("&")) {
            int eq = pair.indexOf('=');
            if (eq >= 0 && name.equals(pair.substring(0, eq).trim())) {
                return decodeComponent(pair.substring(eq + 1).trim());
            }
        }
        return null;
    }

    private static String decodeComponent(String s) {
        if (s == null) {
            return null;
        }
        return java.net.URLDecoder.decode(s, StandardCharsets.UTF_8);
    }

    private static boolean isInternalRead(Request request) {
        String v = request.getHeaders().get(INTERNAL_READ_HEADER);
        return v != null && ("1".equals(v) || "true".equalsIgnoreCase(v));
    }

    private static boolean isInternalReplication(Request request) {
        String v = request.getHeaders().get(INTERNAL_REPLICATION_HEADER);
        return v != null && ("1".equals(v) || "true".equalsIgnoreCase(v));
    }

    private static CompletableFuture<byte[]> readBody(Request request, int maxLength) {
        return Content.Source.asStringAsync(request, StandardCharsets.UTF_8)
                .thenApply(s -> s.getBytes(StandardCharsets.UTF_8))
                .thenCompose(bytes -> bytes.length > maxLength
                        ? CompletableFuture.failedFuture(new IllegalStateException("Request body exceeds " + maxLength))
                        : CompletableFuture.completedFuture(bytes));
    }

    private void writeResponse(Response response, HttpResponse resp, boolean keepAlive, Callback callback) {
        response.setStatus(resp.statusCode());
        response.getHeaders().put("X-Content-Type-Options", "nosniff");
        response.getHeaders().put("X-Frame-Options", "DENY");
        if (resp.contentType() != null) {
            response.getHeaders().put("Content-Type", resp.contentType());
        }
        byte[] body = resp.body() != null && resp.body().length > 0 ? resp.body() : new byte[0];
        response.getHeaders().put("Content-Length", body.length);
        if (keepAlive) {
            response.getHeaders().put("Connection", "keep-alive");
        }
        if (body.length == 0) {
            callback.succeeded();
            return;
        }
        response.write(true, ByteBuffer.wrap(body), callback);
    }

    @FunctionalInterface
    private interface RouteHandler {
        HttpResponse handle(String method, String path, String queryString, byte[] body,
                boolean internalRead, boolean internalReplication);
    }

    private final class DispatchHandler extends Handler.Abstract {

        private final RouteHandler routeHandler;

        DispatchHandler(RouteHandler routeHandler) {
            this.routeHandler = routeHandler;
        }

        @Override
        public boolean handle(Request request, Response response, Callback callback) throws Exception {
            String path = request.getHttpURI().getPath();
            int q = path.indexOf('?');
            String pathOnly = q >= 0 ? path.substring(0, q) : path;
            String queryString = q >= 0 ? path.substring(q + 1) : "";
            String method = request.getMethod();
            boolean internalRead = isInternalRead(request);
            boolean internalReplication = isInternalReplication(request);
            boolean keepAlive = "keep-alive".equalsIgnoreCase(request.getHeaders().get("Connection"));

            CompletableFuture<byte[]> bodyFuture = readBody(request, MAX_CONTENT_LENGTH);
            bodyFuture.whenComplete((body, ex) -> {
                if (ex != null) {
                    log.debug("Request body read failed", ex);
                    callback.failed(ex);
                    return;
                }
                byte[] bodyBytes = (body != null ? body : new byte[0]);
                final byte[] finalBody = bodyBytes.length > 0 ? bodyBytes : null;
                requestExecutor.execute(() -> {
                    HttpResponse resolvedResp;
                    try {
                        resolvedResp = routeHandler.handle(method, pathOnly, queryString, finalBody, internalRead,
                                internalReplication);
                    } catch (Exception e) {
                        log.error("Request handling error", e);
                        resolvedResp = HttpResponse.json(500, Map.of("error", "internal server error"));
                    }
                    try {
                        writeResponse(response, resolvedResp, keepAlive, callback);
                    } catch (Throwable t) {
                        callback.failed(t);
                    }
                });
            });
            return true;
        }
    }
}
