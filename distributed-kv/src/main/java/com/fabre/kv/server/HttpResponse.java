package com.fabre.kv.server;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

/**
 * Immutable HTTP response: status code, optional Content-Type, optional body.
 */
public record HttpResponse(int statusCode, String contentType, byte[] body) {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String APPLICATION_JSON = "application/json";

    public static HttpResponse noContent() {
        return new HttpResponse(204, null, null);
    }

    public static HttpResponse json(int statusCode, Map<String, ?> data) {
        try {
            byte[] bytes = JSON.writeValueAsBytes(data);
            return new HttpResponse(statusCode, APPLICATION_JSON, bytes);
        } catch (Exception e) {
            return new HttpResponse(500, APPLICATION_JSON,
                    "{\"error\":\"serialization failed\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
    }

    /** JSON response with arbitrary body (e.g. list for /admin/nodes). */
    public static HttpResponse json(int statusCode, Object body) {
        try {
            byte[] bytes = JSON.writeValueAsBytes(body);
            return new HttpResponse(statusCode, APPLICATION_JSON, bytes);
        } catch (Exception e) {
            return new HttpResponse(500, APPLICATION_JSON,
                    "{\"error\":\"serialization failed\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
    }
}
