package com.fabre.kv.string;

/**
 * URL normalization without regex. Trims trailing slashes for consistent comparison and path building (avoids
 * Pattern.compile/Matcher on hot paths).
 */
public final class UrlNormalizer {

    private UrlNormalizer() {
    }

    /**
     * Normalizes URL for comparison: trims leading/trailing whitespace and removes trailing slashes. Returns empty
     * string if null or blank.
     */
    public static String normalize(String url) {
        if (url == null)
            return "";
        url = url.trim();
        if (url.isEmpty())
            return "";
        int end = url.length();
        while (end > 0 && url.charAt(end - 1) == '/') {
            end--;
        }
        return end == url.length() ? url : url.substring(0, end);
    }

    /**
     * Ensures the URL has an http or https scheme so it can be used with URI.create. If the URL is null or blank,
     * returns it as-is. If it already starts with "http://" or "https://", returns the normalized URL. If it starts
     * with "://", prepends "http". Otherwise prepends "http://".
     */
    public static String ensureScheme(String url) {
        if (url == null || url.isBlank())
            return url;
        url = normalize(url);
        if (url.startsWith("http://") || url.startsWith("https://"))
            return url;
        if (url.startsWith("://"))
            return "http" + url;
        return "http://" + url;
    }
}
