package tech.callisto.iris.transport;

/**
 * Pluggable transport interface — swap Postgres for any future backend
 * without changing routing, cycle guard, or handler logic.
 */
public interface IrisTransport {
    void connect();
    void subscribe(String channel, TransportListener listener);
    void unsubscribe(String channel);
    void disconnect();
    boolean isConnected();
}
