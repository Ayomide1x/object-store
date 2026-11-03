package io.github.ayomide1x.objectstore.coordinator;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import io.github.ayomide1x.objectstore.v1.CoordinatorServiceGrpc;
import io.github.ayomide1x.objectstore.v1.GetClusterStatusRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.event.EventListener;
import org.springframework.grpc.server.lifecycle.GrpcServerStartedEvent;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * The coordinator's server permits its clients' keepalive pings: nodes'
 * heartbeat connections and gateways ping every 10 s, with or without calls.
 * Against the real server (its application.yaml policy), through a proxy that
 * counts connections: a client pinging at that rate while idle keeps its one
 * connection. Under grpc-java's defaults (pings every 5 min at most, none
 * without calls) the third ping, at about 30 s, gets "too many pings" and the
 * connection is closed.
 */
@Testcontainers
@SpringBootTest(properties = {
        "spring.grpc.server.port=0",
        "coordinator.liveness-sweep=0",
        "coordinator.repair-interval=0",
        "coordinator.gc-interval=0",
})
class CoordinatorPingPolicyIT {

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18");

    static final FakeNodes nodes = new FakeNodes(4);
    static volatile int grpcPort;

    @DynamicPropertySource
    static void registerNodes(DynamicPropertyRegistry registry) {
        registry.add("coordinator.nodes", nodes::nodesProperty);
    }

    @TestConfiguration
    static class Port {
        @EventListener
        void started(GrpcServerStartedEvent e) {
            grpcPort = e.getPort();
        }
    }

    @Test
    @Timeout(90)
    void pingsAtTheClientsRateWithoutCallsKeepTheConnection() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (HalfOpenProxy counter = new HalfOpenProxy(new InetSocketAddress(loopback, grpcPort))) {
            ManagedChannel ch = ManagedChannelBuilder.forAddress(loopback.getHostAddress(), counter.port())
                    .usePlaintext()
                    .keepAliveTime(10, TimeUnit.SECONDS) // the clients' rate (heartbeat, gateway)
                    .keepAliveTimeout(2, TimeUnit.SECONDS)
                    .keepAliveWithoutCalls(true)
                    .build();
            try {
                status(ch);
                Thread.sleep(36_000); // idle: pings at about 10, 20 and 30 s
                status(ch);
                assertThat(counter.accepted()).as("connections after 36 s of pings").isEqualTo(1);
            } finally {
                ch.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            }
        }
    }

    static void status(ManagedChannel ch) {
        CoordinatorServiceGrpc.newBlockingStub(ch).withDeadlineAfter(5, TimeUnit.SECONDS)
                .getClusterStatus(GetClusterStatusRequest.getDefaultInstance());
    }
}
