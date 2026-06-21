package tech.callisto.iris.connector.pg;

public class PgTransportConfig {

    private final String host;
    private final int port;
    private final String database;
    private final String username;
    private final String password;
    private final long initialBackoffMs;
    private final long maxBackoffMs;
    private final int maxRetries;

    private PgTransportConfig(Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.database = builder.database;
        this.username = builder.username;
        this.password = builder.password;
        this.initialBackoffMs = builder.initialBackoffMs;
        this.maxBackoffMs = builder.maxBackoffMs;
        this.maxRetries = builder.maxRetries;
    }

    public String getHost() { return host; }
    public int getPort() { return port; }
    public String getDatabase() { return database; }
    public String getUsername() { return username; }
    public String getPassword() { return password; }
    public long getInitialBackoffMs() { return initialBackoffMs; }
    public long getMaxBackoffMs() { return maxBackoffMs; }
    public int getMaxRetries() { return maxRetries; }

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private String host = "localhost";
        private int port = 5432;
        private String database;
        private String username;
        private String password;
        private long initialBackoffMs = 1000;
        private long maxBackoffMs = 30000;
        private int maxRetries = Integer.MAX_VALUE;

        public Builder host(String host) { this.host = host; return this; }
        public Builder port(int port) { this.port = port; return this; }
        public Builder database(String database) { this.database = database; return this; }
        public Builder username(String username) { this.username = username; return this; }
        public Builder password(String password) { this.password = password; return this; }
        public Builder initialBackoffMs(long ms) { this.initialBackoffMs = ms; return this; }
        public Builder maxBackoffMs(long ms) { this.maxBackoffMs = ms; return this; }
        public Builder maxRetries(int retries) { this.maxRetries = retries; return this; }
        public PgTransportConfig build() { return new PgTransportConfig(this); }
    }
}
