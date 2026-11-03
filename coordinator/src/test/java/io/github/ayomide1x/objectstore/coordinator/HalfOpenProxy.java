package io.github.ayomide1x.objectstore.coordinator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A TCP proxy on loopback whose existing connections can be made half-open:
 * after {@link #freeze()}, bytes on them are read and dropped in both
 * directions and nothing is closed, so each end still holds an established
 * connection that never delivers again. Connections accepted after the freeze
 * forward normally, as a reconnect over a healthy path would.
 */
final class HalfOpenProxy implements AutoCloseable {

    private final ServerSocket server;
    private final InetSocketAddress target;
    private final AtomicInteger accepted = new AtomicInteger();
    private final List<Pair> pairs = Collections.synchronizedList(new ArrayList<>());
    private final Thread acceptor;

    private static final class Pair {
        final Socket client;
        final Socket upstream;
        volatile boolean frozen;

        Pair(Socket client, Socket upstream) {
            this.client = client;
            this.upstream = upstream;
        }

        void close() {
            try {
                client.close();
            } catch (IOException e) {
                // closing anyway
            }
            try {
                upstream.close();
            } catch (IOException e) {
                // closing anyway
            }
        }
    }

    HalfOpenProxy(InetSocketAddress target) throws IOException {
        this.target = target;
        this.server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        this.acceptor = Thread.ofVirtual().start(this::acceptLoop);
    }

    int port() {
        return server.getLocalPort();
    }

    /** Connections accepted so far. */
    int accepted() {
        return accepted.get();
    }

    /** Makes every connection open now half-open. */
    void freeze() {
        synchronized (pairs) {
            pairs.forEach(p -> p.frozen = true);
        }
    }

    private void acceptLoop() {
        try {
            while (true) {
                Socket client = server.accept();
                accepted.incrementAndGet();
                Socket upstream = new Socket(target.getAddress(), target.getPort());
                Pair p = new Pair(client, upstream);
                pairs.add(p);
                Thread.ofVirtual().start(() -> pump(p, client, upstream));
                Thread.ofVirtual().start(() -> pump(p, upstream, client));
            }
        } catch (IOException e) {
            // closed
        }
    }

    private static void pump(Pair p, Socket from, Socket to) {
        byte[] buf = new byte[16 * 1024];
        try {
            InputStream in = from.getInputStream();
            OutputStream out = to.getOutputStream();
            int n;
            while ((n = in.read(buf)) >= 0) {
                if (!p.frozen) {
                    out.write(buf, 0, n);
                    out.flush();
                }
            }
        } catch (IOException e) {
            // a side closed
        }
        if (!p.frozen) {
            p.close(); // a normal close is passed on; a frozen connection closes nothing
        }
    }

    @Override
    public void close() throws Exception {
        server.close();
        acceptor.join();
        synchronized (pairs) {
            pairs.forEach(Pair::close);
        }
    }
}
