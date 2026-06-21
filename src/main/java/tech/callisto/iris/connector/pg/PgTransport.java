package tech.callisto.iris.connector.pg;

import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.api.jdbc.PGNotificationListener;
import com.impossibl.postgres.jdbc.PGDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.callisto.iris.transport.IrisTransport;
import tech.callisto.iris.transport.TransportListener;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class PgTransport implements IrisTransport {

    private static final Logger log = LoggerFactory.getLogger(PgTransport.class);

    private final PgTransportConfig config;
    private final Map<String, TransportListener> channelRegistry = new ConcurrentHashMap<>();
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final ScheduledExecutorService reconnectScheduler = Executors.newSingleThreadScheduledExecutor();

    // Notifications arrive on pgjdbc-ng I/O threads — dispatch off that thread
    // immediately to avoid blocking the notification pipeline (per section 9.2 of pgjdbc-ng user guide)
    private final ExecutorService dispatchExecutor = Executors.newCachedThreadPool();

    private PGConnection connection;

    public PgTransport(PgTransportConfig config) {
        this.config = config;
    }

    @Override
    public void connect() {
        attemptConnect(0);
    }

    private void attemptConnect(int attempt) {
        try {
            PGDataSource ds = new PGDataSource();
            ds.setHost(config.getHost());
            ds.setPort(config.getPort());
            ds.setDatabaseName(config.getDatabase());
            ds.setUser(config.getUsername());
            ds.setPassword(config.getPassword());

            connection = ds.getConnection().unwrap(PGConnection.class);

            // Section 9.2 — Asynchronous Notifications (pgjdbc-ng user guide)
            // addNotificationListener() registers for all channels; closed() fires on unexpected disconnect
            connection.addNotificationListener(new PGNotificationListener() {
                @Override
                public void notification(int processId, String channelName, String payload) {
                    TransportListener listener = channelRegistry.get(channelName);
                    if (listener != null) {
                        // dispatch off the I/O thread — long or blocking ops here degrade performance
                        dispatchExecutor.submit(() -> listener.onEvent(channelName, payload));
                    }
                }

                @Override
                public void closed() {
                    log.warn("Postgres connection closed — scheduling reconnect");
                    connected.set(false);
                    scheduleReconnect(0);
                }
            });

            reRegisterChannels();
            connected.set(true);
            log.info("Iris connected to Postgres at {}:{}/{}", config.getHost(), config.getPort(), config.getDatabase());

        } catch (SQLException e) {
            connected.set(false);
            log.warn("Connection attempt {} failed: {} — retrying", attempt + 1, e.getMessage());
            scheduleReconnect(attempt + 1);
        }
    }

    private void scheduleReconnect(int attempt) {
        if (attempt >= config.getMaxRetries()) {
            log.error("Max reconnect attempts reached. Iris shutting down transport.");
            return;
        }
        long backoff = Math.min(config.getInitialBackoffMs() * (1L << Math.min(attempt, 10)), config.getMaxBackoffMs());
        log.info("Reconnecting in {}ms (attempt {})", backoff, attempt + 1);
        reconnectScheduler.schedule(() -> attemptConnect(attempt), backoff, TimeUnit.MILLISECONDS);
    }

    private void reRegisterChannels() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            for (String channel : channelRegistry.keySet()) {
                stmt.execute("LISTEN " + channel);
                log.info("Re-registered LISTEN on channel: {}", channel);
            }
        }
    }

    @Override
    public void subscribe(String channel, TransportListener listener) {
        channelRegistry.put(channel, listener);
        if (connected.get()) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("LISTEN " + channel);
                log.info("Subscribed to channel: {}", channel);
            } catch (SQLException e) {
                log.error("Failed to LISTEN on channel {}: {}", channel, e.getMessage());
            }
        }
    }

    @Override
    public void unsubscribe(String channel) {
        channelRegistry.remove(channel);
        if (connected.get()) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("UNLISTEN " + channel);
                log.info("Unsubscribed from channel: {}", channel);
            } catch (SQLException e) {
                log.error("Failed to UNLISTEN on channel {}: {}", channel, e.getMessage());
            }
        }
    }

    @Override
    public void disconnect() {
        reconnectScheduler.shutdownNow();
        dispatchExecutor.shutdownNow();
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            log.warn("Error closing connection: {}", e.getMessage());
        }
        connected.set(false);
        log.info("Iris disconnected.");
    }

    @Override
    public boolean isConnected() {
        return connected.get();
    }
}
