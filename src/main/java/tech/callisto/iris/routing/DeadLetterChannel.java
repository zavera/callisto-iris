package tech.callisto.iris.routing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DeadLetterChannel {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterChannel.class);

    private final List<FailedEvent> events = new ArrayList<>();

    public void record(String channel, String payload, Throwable cause) {
        FailedEvent event = new FailedEvent(channel, payload, cause);
        events.add(event);
        log.error("Dead letter — channel: {}, cause: {}", channel, cause.getMessage());
    }

    public List<FailedEvent> getEvents() {
        return List.copyOf(events);
    }

    public record FailedEvent(String channel, String payload, Throwable cause) {}
}
