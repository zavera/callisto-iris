package tech.callisto.iris.routing;

/**
 * Implement this to handle events routed from a specific channel.
 * The payload is the raw JSON string from pg_notify.
 */
@FunctionalInterface
public interface IrisHandler {
    void handle(String channel, String payload) throws Exception;
}
