package io.github.ayomide1x.objectstore.coordinator;

import java.security.Security;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class CoordinatorApplication {

    public static void main(String[] args) {
        configureDnsCache(System.getenv("COORDINATOR_DNS_CACHE_TTL_SECONDS"));
        SpringApplication.run(CoordinatorApplication.class, args);
    }

    /**
     * Shortens the JVM's DNS caching, which defaults to 30 s for successful
     * lookups and 10 s for failed ones. Node containers can get new IPs when
     * they restart, and a cached address keeps the coordinator calling the
     * wrong container. Must run before the first lookup.
     *
     * @param ttlSeconds {@code COORDINATOR_DNS_CACHE_TTL_SECONDS}; default 1
     */
    static void configureDnsCache(String ttlSeconds) {
        String ttl = ttlSeconds == null || ttlSeconds.isBlank() ? "1" : ttlSeconds.trim();
        if (Integer.parseInt(ttl) < 0) {
            throw new IllegalStateException("COORDINATOR_DNS_CACHE_TTL_SECONDS must be >= 0, got " + ttl);
        }
        Security.setProperty("networkaddress.cache.ttl", ttl);
        Security.setProperty("networkaddress.cache.negative.ttl", ttl);
        // grpc-java's DNS resolver reads this system property for how long it
        // keeps a resolution before resolving again.
        System.setProperty("networkaddress.cache.ttl", ttl);
    }
}
