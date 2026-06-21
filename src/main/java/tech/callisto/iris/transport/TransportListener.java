package tech.callisto.iris.transport;

@FunctionalInterface
public interface TransportListener {
    void onEvent(String channel, String payload);
}
