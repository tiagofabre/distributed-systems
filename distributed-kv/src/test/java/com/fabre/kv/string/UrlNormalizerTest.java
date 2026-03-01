package com.fabre.kv.string;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("UrlNormalizer")
class UrlNormalizerTest {

    @Test
    @DisplayName("ensureScheme prepends http when URL starts with :// (regression)")
    void ensureSchemePrependsHttpWhenStartsWithColonSlashSlash() {
        assertEquals("http://127.0.0.1:8203", UrlNormalizer.ensureScheme("://127.0.0.1:8203"));
        assertEquals("http://host:80", UrlNormalizer.ensureScheme("://host:80"));
    }

    @Test
    @DisplayName("ensureScheme leaves http and https URLs unchanged")
    void ensureSchemeLeavesHttpAndHttpsUnchanged() {
        assertEquals("http://127.0.0.1:8201", UrlNormalizer.ensureScheme("http://127.0.0.1:8201"));
        assertEquals("https://example.com", UrlNormalizer.ensureScheme("https://example.com"));
        assertEquals("http://n1", UrlNormalizer.ensureScheme("http://n1/"));
    }

    @Test
    @DisplayName("ensureScheme prepends http:// when no scheme")
    void ensureSchemePrependsHttpWhenNoScheme() {
        assertEquals("http://127.0.0.1:8201", UrlNormalizer.ensureScheme("127.0.0.1:8201"));
    }

    @Test
    @DisplayName("ensureScheme returns null or blank as-is")
    void ensureSchemeReturnsNullOrBlankAsIs() {
        assertNull(UrlNormalizer.ensureScheme(null));
        assertEquals("", UrlNormalizer.ensureScheme(""));
        assertTrue(UrlNormalizer.ensureScheme("   ").isBlank());
    }

    @Test
    @DisplayName("normalize trims and strips trailing slash")
    void normalizeTrimsAndStripsTrailingSlash() {
        assertEquals("http://n1", UrlNormalizer.normalize("http://n1/"));
        assertEquals("http://n1", UrlNormalizer.normalize("  http://n1  "));
        assertEquals("", UrlNormalizer.normalize(null));
        assertEquals("", UrlNormalizer.normalize(""));
    }
}
