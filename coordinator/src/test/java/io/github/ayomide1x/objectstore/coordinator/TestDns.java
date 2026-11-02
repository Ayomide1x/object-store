package io.github.ayomide1x.objectstore.coordinator;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.spi.InetAddressResolver;
import java.net.spi.InetAddressResolverProvider;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

/**
 * A DNS the tests control, installed for the whole test JVM through the JDK's
 * resolver SPI. Names ending in ".test" resolve to what a test sets, and a
 * test can hold lookups until it releases them; every other name goes to the
 * system resolver.
 */
public final class TestDns extends InetAddressResolverProvider {

    static final Map<String, InetAddress> names = new ConcurrentHashMap<>();
    /** While set, lookups of .test names wait for it to be counted down. */
    static volatile CountDownLatch hold;
    static final java.util.concurrent.atomic.AtomicInteger lookups = new java.util.concurrent.atomic.AtomicInteger();

    @Override
    public InetAddressResolver get(Configuration configuration) {
        InetAddressResolver system = configuration.builtinResolver();
        return new InetAddressResolver() {
            @Override
            public Stream<InetAddress> lookupByName(String host, LookupPolicy policy) throws UnknownHostException {
                if (!host.endsWith(".test")) {
                    return system.lookupByName(host, policy);
                }
                lookups.incrementAndGet();
                CountDownLatch h = hold;
                if (h != null) {
                    try {
                        h.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                InetAddress a = names.get(host);
                if (a == null) {
                    throw new UnknownHostException(host);
                }
                return Stream.of(InetAddress.getByAddress(host, a.getAddress()));
            }

            @Override
            public String lookupByAddress(byte[] addr) throws UnknownHostException {
                return system.lookupByAddress(addr);
            }
        };
    }

    @Override
    public String name() {
        return "objectstore-test-dns";
    }

    static void reset() {
        names.clear();
        hold = null;
    }

    static List<String> testNames() {
        return List.copyOf(names.keySet());
    }
}
