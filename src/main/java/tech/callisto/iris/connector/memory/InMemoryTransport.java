package tech.callisto.iris.connector.memory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.callisto.iris.transport.IrisTransport;
import tech.callisto.iris.transport.TransportListener;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * In-memory transport -- no database required.
 * Events are published programmatically via publish().
 * Drop-in replacement for PgTransport during development or when no DB trigger is available.
 * Swap for PgTransport when Postgres is added.
 */
public class InMemoryTransport implements IrisTransport {

    private static final Logger log = LoggerFactory.getLogger(InMemoryTransport.class);

    private final Map<String, TransportListener> registry = new ConcurrentHashMap<>();
    private final ExecutorService dispatcher = Executors.newCachedThreadPool();
    private volatile boolean connected = false;

    @Override
    public void connect() {
        connected = true;
        log.info("Iris InMemoryTransport connected.");
    }

    @Override
    public void subscribe(String channel, TransportListener listener) {
        registry.put(channel, listener);
        log.info("Iris subscribed to in-memory channel: {}", channel);
    }

    @Override
    public void unsubscribe(String channel) {
        registry.remove(channel);
        log.info("Iris unsubscribed from in-memory channel: {}", channel);
    }

    @Override
    public void disconnect() {
        dispatcher.shutdownNow();
        connected = false;
        log.info("Iris InMemoryTransport disconnected.");
    }

    @Override
    public boolean isConnected() {
        return connected;
    }

    public void publish(String channel, String payload) {
        TransportListener listener = registry.get(channel);
        if (listener != null) {
            dispatcher.submit(() -> listener.onEvent(channel, payload));
        } else {
            log.warn("No listener registered for channel: {}", channel);
        }
    }
}
